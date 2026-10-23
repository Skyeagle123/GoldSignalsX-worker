/* Test-only Linux egress isolation, inherited across exec and subprocesses.
 * Reject socket/socketpair unless AF_UNIX. No inherited network descriptors. */
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <linux/audit.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <stddef.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
int main(int argc,char **argv){
 if(argc<2)return 2;
 struct sock_filter f[]={
 BPF_STMT(BPF_LD|BPF_W|BPF_ABS,offsetof(struct seccomp_data,arch)),
 BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K,AUDIT_ARCH_X86_64,1,0),
 BPF_STMT(BPF_RET|BPF_K,SECCOMP_RET_KILL_PROCESS),
 BPF_STMT(BPF_LD|BPF_W|BPF_ABS,offsetof(struct seccomp_data,nr)),
 BPF_JUMP(BPF_JMP|BPF_JGE|BPF_K,0x40000000,0,1),
 BPF_STMT(BPF_RET|BPF_K,SECCOMP_RET_KILL_PROCESS),
 BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K,SYS_io_uring_setup,0,1),
 BPF_STMT(BPF_RET|BPF_K,SECCOMP_RET_ERRNO|EPERM),
 BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K,SYS_socket,2,0),
 BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K,SYS_socketpair,1,0),
 BPF_STMT(BPF_RET|BPF_K,SECCOMP_RET_ALLOW),
 BPF_STMT(BPF_LD|BPF_W|BPF_ABS,offsetof(struct seccomp_data,args[0])),
 BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K,AF_UNIX,0,1),
 BPF_STMT(BPF_RET|BPF_K,SECCOMP_RET_ALLOW),
 BPF_STMT(BPF_RET|BPF_K,SECCOMP_RET_ERRNO|EPERM)};
 struct sock_fprog p={sizeof(f)/sizeof(f[0]),f};
 if(prctl(PR_SET_NO_NEW_PRIVS,1,0,0,0)||prctl(PR_SET_SECCOMP,SECCOMP_MODE_FILTER,&p)){perror("seccomp");return 3;}
 execvp(argv[1],argv+1);perror("exec");return 4;
}
