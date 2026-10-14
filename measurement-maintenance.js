// Internal maintenance composition, never installed on fetch/report handlers.
import {enabled,need,independentBindings,time,identity} from './measurement-independent-contract.js';
import {assertion,clearAssertions,SQL_CLOCK} from './measurement-d1-adapter.js';
import {publishArchive,archiveReader} from './measurement-archive.js';
import {verifyArchiveParity,evictPublication} from './measurement-independent-retention.js';
import {admitCorrection,processCorrection} from './measurement-corrections.js';
import {hydrateArchive} from './measurement-restore.js';
import {authorizeReplay} from './measurement-queue-consumer.js';
export function createMeasurementMaintenance(db,{archive,env,authorization,clock=()=>Date.now()}={}){
 independentBindings(env,'measurement');const environment=env.MEASUREMENT_ENVIRONMENT,reader=archive?archiveReader(archive,{getRoot:async()=>(await db.first('SELECT root_key FROM pa_meta')).root_key}):null;
 need(environment==='synthetic-b1-package-a'&&authorization?.environment===environment&&Array.isArray(authorization.operations)&&authorization.operations.length<=8,'pa_maintenance_authorization');
 const authorized=async(operation,gate)=>{const now=time(clock());need(enabled(env,gate)&&authorization.operations.includes(operation)&&authorization.expiresAt>now,'pa_maintenance_off_or_unauthorized');await db.batch([assertion(db,`? > MAX(?,${SQL_CLOCK}) AND EXISTS(SELECT 1 FROM pa_environment WHERE singleton=1 AND plane='MEASUREMENT' AND environment=? AND resource_id=?)`,authorization.expiresAt,now,environment,env.MEASUREMENT_EXPECTED_D1_ID),clearAssertions(db)]);return now;};
 return Object.freeze({
  async publish(scopeId){const now=await authorized('archive','B1_ARCHIVE_WRITE_ENABLED');return publishArchive(db,archive,identity(scopeId),{now});},
  async parity(publicationId){await authorized('archive','B1_ARCHIVE_WRITE_ENABLED');return verifyArchiveParity(db,archive,identity(publicationId));},
  async evict(publicationId){const now=await authorized('evict','B1_ARCHIVE_EVICTION_ENABLED');return evictPublication(db,archive,identity(publicationId),{now});},
  async correct(input){const now=await authorized('correction','B1_CORRECTION_ADMISSION_ENABLED');return admitCorrection(db,input,{now,archive:reader});},
  async processCorrection(id){const now=await authorized('correction','B1_CORRECTION_ADMISSION_ENABLED');return processCorrection(db,identity(id),{now,archive:reader});},
  async restore(rootKey,options={}){const now=await authorized('restore','B1_RESTORE_ENABLED');return hydrateArchive(db,archive,rootKey,{ids:options.ids??null,now,environment,authorization:{operation:'restore',environment,expiresAt:authorization.expiresAt}});},
  async replay(id){const now=await authorized('replay','B1_MEASUREMENT_CONSUMER_ENABLED');return authorizeReplay(db,identity(id),{now,environment,authorization:{operation:'replay',environment,expiresAt:authorization.expiresAt}});}
 });
}
