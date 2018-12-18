import com.google.gson.Gson
import com.binance.api.client.domain.market.Candlestick
import com.huobi.response.Kline
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets

flowFile = session.get()
if (!flowFile) return


gson = new Gson()
exchangeName = flowFile.getAttribute('kafka.topic').split("-")[0]
payload = ''
session.read(flowFile, {inputStream ->
    payload = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
} as InputStreamCallback)

StandardKline standardKline
switch (exchangeName) {
    case "huobi":
        Kline kline = gson.fromJson(payload, Kline.class)
        standardKline = new StandardKline(
                exchangeName.toString(),
                kline.id.toString(),
                kline.open.doubleValue(),
                kline.close.doubleValue(),
                kline.low.doubleValue(),
                kline.high.doubleValue(),
                kline.vol,
                kline.amount,
                kline.count.doubleValue())
        break
    case "binance":
        Candlestick candlestick = gson.fromJson(payload, Candlestick.class)
        standardKline = new StandardKline(
                exchangeName.toString(),
                (candlestick.getOpenTime()/1000L).toString(),
                candlestick.getOpen().toDouble(),
                candlestick.getClose().toDouble(),
                candlestick.getLow().toDouble(),
                candlestick.getHigh().toDouble(),
                candlestick.getVolume().toDouble(),
                0,
                candlestick.getNumberOfTrades().toDouble())
        break
    default:
        throw new IllegalArgumentException("Unrecognized exchange name " + exchangeName)
        break
}

session.write(flowFile, {outputStream ->
    outputStream.write(gson.toJson(standardKline).getBytes(StandardCharsets.UTF_8))
} as OutputStreamCallback)
session.transfer(flowFile, REL_SUCCESS)

class StandardKline {

    String timestamp
    double close
    double open
    double low
    double high
    double volume
    String exchange
    double amount
    double count

    StandardKline(String exchange,
                  String timestamp,
                  double open,
                  double close,
                  double low,
                  double high,
                  double volume,
                  double amount,
                  double count){
        this.timestamp = timestamp
        this.open = open
        this.close = close
        this.low = low
        this.high = high
        this.volume = volume
        this.exchange = exchange
        this.amount = amount
        this.count = count
    }
}
