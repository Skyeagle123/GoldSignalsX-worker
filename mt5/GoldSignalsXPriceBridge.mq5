#property copyright "GoldSignalsX"
#property version   "1.00"
#property strict

input string InpWorkerUrl      = "https://goldsignalsx-worker.example.workers.dev/mt5/tick";
input string InpMt5Token       = "";
input string InpMt5Symbol      = "";
input string InpCanonicalSymbol= "XAUUSD";
input int    InpIntervalMs     = 1000;
input int    InpTimeoutMs      = 5000;
input int    InpMetadataIntervalMs = 300000;

ulong  bridgeSequence=0;
ulong  bridgeMetadataSentAt=0;
string bridgeSessionId="";

string JsonEscape(const string value)
{
   string escaped=value;
   StringReplace(escaped,"\\","\\\\");
   StringReplace(escaped,"\"","\\\"");
   return escaped;
}

string JsonNumber(const double value)
{
   if(!MathIsValidNumber(value))
      return "null";
   return DoubleToString(value,8);
}

string BuildSymbolMetadata(const string mt5Symbol,const long observedAtMs)
{
   const double contractSize=SymbolInfoDouble(mt5Symbol,SYMBOL_TRADE_CONTRACT_SIZE);
   const double tickSize=SymbolInfoDouble(mt5Symbol,SYMBOL_TRADE_TICK_SIZE);
   const double tickValue=SymbolInfoDouble(mt5Symbol,SYMBOL_TRADE_TICK_VALUE);
   const double tickValueProfit=SymbolInfoDouble(mt5Symbol,SYMBOL_TRADE_TICK_VALUE_PROFIT);
   const double tickValueLoss=SymbolInfoDouble(mt5Symbol,SYMBOL_TRADE_TICK_VALUE_LOSS);
   const double volumeMin=SymbolInfoDouble(mt5Symbol,SYMBOL_VOLUME_MIN);
   const double volumeMax=SymbolInfoDouble(mt5Symbol,SYMBOL_VOLUME_MAX);
   const double volumeStep=SymbolInfoDouble(mt5Symbol,SYMBOL_VOLUME_STEP);
   const double volumeLimit=SymbolInfoDouble(mt5Symbol,SYMBOL_VOLUME_LIMIT);
   const double point=SymbolInfoDouble(mt5Symbol,SYMBOL_POINT);
   const long digits=SymbolInfoInteger(mt5Symbol,SYMBOL_DIGITS);
   const long stopsLevel=SymbolInfoInteger(mt5Symbol,SYMBOL_TRADE_STOPS_LEVEL);
   const ENUM_SYMBOL_CALC_MODE calcMode=(ENUM_SYMBOL_CALC_MODE)SymbolInfoInteger(mt5Symbol,SYMBOL_TRADE_CALC_MODE);
   const string calcModeName=EnumToString(calcMode);
   const long tradeMode=SymbolInfoInteger(mt5Symbol,SYMBOL_TRADE_MODE);
   const string profitCurrency=SymbolInfoString(mt5Symbol,SYMBOL_CURRENCY_PROFIT);
   const string accountCurrency=AccountInfoString(ACCOUNT_CURRENCY);
   return StringFormat(
      "{\"source\":\"mt5\",\"canonicalSymbol\":\"%s\",\"brokerSymbol\":\"%s\",\"sessionId\":\"%s\",\"observedAt\":%I64d,\"contractSize\":%s,\"tickSize\":%s,\"tickValue\":%s,\"tickValueProfit\":%s,\"tickValueLoss\":%s,\"volumeMin\":%s,\"volumeMax\":%s,\"volumeStep\":%s,\"volumeLimit\":%s,\"point\":%s,\"digits\":%I64d,\"tradeStopsLevel\":%I64d,\"profitCurrency\":\"%s\",\"accountCurrency\":\"%s\",\"tradeCalcMode\":\"%s\",\"tradeMode\":%I64d}",
      JsonEscape(InpCanonicalSymbol),JsonEscape(mt5Symbol),JsonEscape(bridgeSessionId),observedAtMs,
      JsonNumber(contractSize),JsonNumber(tickSize),JsonNumber(tickValue),
      JsonNumber(tickValueProfit),JsonNumber(tickValueLoss),
      JsonNumber(volumeMin),JsonNumber(volumeMax),JsonNumber(volumeStep),JsonNumber(volumeLimit),
      JsonNumber(point),digits,stopsLevel,JsonEscape(profitCurrency),JsonEscape(accountCurrency),JsonEscape(calcModeName),tradeMode
   );
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
   const long sentAtSeconds=(long)TimeGMT();
   const long serverUtcOffsetMs=((long)TimeTradeServer()-sentAtSeconds)*1000;
   const long rawMt5TimeMs=tick.time_msc>0?tick.time_msc:(long)tick.time*1000;
   const long mt5TimeMs=rawMt5TimeMs-serverUtcOffsetMs;
   const long sentAtMs=sentAtSeconds*1000;
   const int digits=(int)SymbolInfoInteger(mt5Symbol,SYMBOL_DIGITS);
   const string bid=DoubleToString(tick.bid,digits);
   const string ask=DoubleToString(tick.ask,digits);
   const ulong monotonicNow=GetTickCount64();
   const ulong metadataInterval=(ulong)MathMax(60000,InpMetadataIntervalMs);
   const bool includeMetadata=(bridgeMetadataSentAt==0 || monotonicNow-bridgeMetadataSentAt>=metadataInterval);
   const string metadataSuffix=includeMetadata
      ? ",\"symbolMeta\":"+BuildSymbolMetadata(mt5Symbol,sentAtMs)
      : "";
   const string payload=StringFormat(
      "{\"symbol\":\"%s\",\"bid\":%s,\"ask\":%s,\"mt5Time\":%I64d,\"sentAt\":%I64d,\"sequence\":%I64u,\"sessionId\":\"%s\",\"source\":\"mt5\"%s}",
      JsonEscape(InpCanonicalSymbol),bid,ask,mt5TimeMs,sentAtMs,bridgeSequence,
      JsonEscape(bridgeSessionId),metadataSuffix
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
   {
      PrintFormat("GoldSignalsX bridge ingest rejected with HTTP %d.",status);
      return;
   }
   if(includeMetadata)
      bridgeMetadataSentAt=monotonicNow;
}
