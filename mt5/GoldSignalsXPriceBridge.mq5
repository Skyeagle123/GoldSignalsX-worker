#property copyright "GoldSignalsX"
#property version   "1.00"
#property strict

input string InpWorkerUrl      = "https://goldsignalsx-worker.example.workers.dev/mt5/tick";
input string InpMt5Token       = "";
input string InpMt5Symbol      = "";
input string InpCanonicalSymbol= "XAUUSD";
input int    InpIntervalMs     = 1000;
input int    InpTimeoutMs      = 5000;

ulong  bridgeSequence=0;
string bridgeSessionId="";

string JsonEscape(const string value)
{
   string escaped=value;
   StringReplace(escaped,"\\","\\\\");
   StringReplace(escaped,"\"","\\\"");
   return escaped;
}

int OnInit()
{
   if(StringLen(InpWorkerUrl)==0 || StringLen(InpMt5Token)==0)
   {
      Print("GoldSignalsX bridge is not configured: set Worker URL and MT5 ingest token locally.");
      return(INIT_PARAMETERS_INCORRECT);
   }
   if(InpIntervalMs<200)
   {
      Print("GoldSignalsX bridge interval must be at least 200 ms.");
      return(INIT_PARAMETERS_INCORRECT);
   }
   bridgeSessionId=StringFormat("%I64d-%I64u",(long)TimeGMT(),GetTickCount64());
   EventSetMillisecondTimer(InpIntervalMs);
   Print("GoldSignalsX read-only price bridge started.");
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   Print("GoldSignalsX read-only price bridge stopped.");
}

void OnTimer()
{
   const string mt5Symbol=StringLen(InpMt5Symbol)>0?InpMt5Symbol:_Symbol;
   MqlTick tick;
   if(!SymbolInfoTick(mt5Symbol,tick) || tick.bid<=0.0 || tick.ask<=0.0 || tick.ask<tick.bid)
   {
      Print("GoldSignalsX bridge could not read a valid Bid/Ask tick.");
      return;
   }

   bridgeSequence++;
   const long mt5TimeMs=tick.time_msc>0?tick.time_msc:(long)tick.time*1000;
   const long sentAtMs=(long)TimeGMT()*1000;
   const int digits=(int)SymbolInfoInteger(mt5Symbol,SYMBOL_DIGITS);
   const string bid=DoubleToString(tick.bid,digits);
   const string ask=DoubleToString(tick.ask,digits);
   const string payload=StringFormat(
      "{\"symbol\":\"%s\",\"bid\":%s,\"ask\":%s,\"mt5Time\":%I64d,\"sentAt\":%I64d,\"sequence\":%I64u,\"sessionId\":\"%s\",\"source\":\"mt5\"}",
      JsonEscape(InpCanonicalSymbol),bid,ask,mt5TimeMs,sentAtMs,bridgeSequence,JsonEscape(bridgeSessionId)
   );

   char requestBody[];
   const int copied=StringToCharArray(payload,requestBody,0,WHOLE_ARRAY,CP_UTF8);
   if(copied>0) ArrayResize(requestBody,copied-1);
   char responseBody[];
   string responseHeaders="";
   const string headers="Content-Type: application/json\r\nX-MT5-Token: "+InpMt5Token+"\r\n";
   ResetLastError();
   const int status=WebRequest("POST",InpWorkerUrl,headers,InpTimeoutMs,requestBody,responseBody,responseHeaders);
   if(status<0)
   {
      PrintFormat("GoldSignalsX bridge WebRequest failed (%d).",GetLastError());
      return;
   }
   if(status<200 || status>=300)
      PrintFormat("GoldSignalsX bridge ingest rejected with HTTP %d.",status);
}
