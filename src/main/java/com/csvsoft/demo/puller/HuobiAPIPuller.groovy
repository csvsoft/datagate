import com.google.gson.Gson
import com.huobi.api.ApiClient
import com.huobi.response.Kline

import java.nio.charset.StandardCharsets

def huobiAPIAccessKey = huobiAPIAccessKey.evaluateAttributeExpressions().value
def huobiAPISecretKey = huobiAPISecretKey.evaluateAttributeExpressions().value
def symbol = symbol.evaluateAttributeExpressions().value
def period = period.evaluateAttributeExpressions().value
def size = size.evaluateAttributeExpressions().value

def huobiAPIClient = new ApiClient(huobiAPIAccessKey, huobiAPISecretKey)

klines = huobiAPIClient.kline(symbol, period, size).checkAndReturn()
gson = new Gson()

for (Kline kline: klines){
    flowFile = session.create()
    flowFile = session.write(flowFile, {outputStream ->
        outputStream.write(gson.toJson(kline).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
}