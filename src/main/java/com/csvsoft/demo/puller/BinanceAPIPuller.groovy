import com.binance.api.client.domain.market.Candlestick
import com.binance.api.client.domain.market.CandlestickInterval
import com.google.gson.Gson
import com.binance.api.client.BinanceApiClientFactory
import java.nio.charset.StandardCharsets

def binanceAPIAccessKey = binanceAPIAccessKey.evaluateAttributeExpressions().value
def binanceAPISecretKey = binanceAPISecretKey.evaluateAttributeExpressions().value
def symbol = symbol.evaluateAttributeExpressions().value
def period = period.evaluateAttributeExpressions().value
def size = size.evaluateAttributeExpressions().value

def binanceAPIClient = BinanceApiClientFactory.newInstance(binanceAPIAccessKey, binanceAPISecretKey).newRestClient()

klines = binanceAPIClient.getCandlestickBars(symbol, CandlestickInterval.valueOf(period.toString()), Integer.valueOf(size), null, null)
gson = new Gson()

for (Candlestick kline: klines){
    flowFile = session.create()
    flowFile = session.write(flowFile, {outputStream ->
        outputStream.write(gson.toJson(kline).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
}