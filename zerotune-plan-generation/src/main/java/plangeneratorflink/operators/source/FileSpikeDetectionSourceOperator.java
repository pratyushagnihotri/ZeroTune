package plangeneratorflink.operators.source;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;
import plangeneratorflink.utils.RanGen;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.zip.GZIPInputStream;

public class FileSpikeDetectionSourceOperator
        extends AbstractOperator<DataGeneratorSource<DataTuple>>
        implements DataGenerator<DataTuple> {

    private final int eventRate;
    private final String gzipURLDownloadPath;
    private String filePath;
    private InputStream gzipStream;
    private BufferedReader reader;

    public FileSpikeDetectionSourceOperator(String id, String gzipURLDownloadPath) {
        setId(id);
        this.eventRate = RanGen.randIntFromList(Constants.SD.throughputs);
        this.gzipURLDownloadPath = gzipURLDownloadPath;
        this.function = new DataGeneratorSource<>(this, eventRate, null, getDescription());
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put("operatorType", "SourceOperator");
        description.put("confEventRate", eventRate);
        description.put("numInteger", 2);
        description.put("numDouble", 4);
        description.put("numString", 0);
        description.put("tupleWidthOut", 1); // add timestamp value manually
        return description;
    }

    @Override
    public void open(
            String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
            throws Exception {
        String dir = System.getProperty("user.dir");
        String fileName = "spikedetectiondata.gz";
        filePath = dir + "/" + fileName;
        File file = new File(filePath);
        if (file.exists()) {
            System.out.println(
                    "No need to download again, file spike detection already exists at "
                            + filePath);
        } else {
            try {
                URL url = new URL(gzipURLDownloadPath);
                HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
                if (httpConn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    InputStream inputStream = httpConn.getInputStream();

                    FileOutputStream outputStream = new FileOutputStream(filePath);
                    int bytesRead = -1;
                    byte[] buffer = new byte[4096];
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                    outputStream.close();
                    inputStream.close();
                } else {
                    throw new RuntimeException(
                            "Cannot download file ("
                                    + gzipURLDownloadPath
                                    + "): "
                                    + httpConn.getResponseCode());
                }
                httpConn.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Downloaded file spike detection to " + filePath);
        }
        gzipStream = new GZIPInputStream(new FileInputStream(filePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public DataTuple next() {
        DataTuple sensorRecord = null;
        while (sensorRecord == null) {
            try {
                String sensorReadingString = reader.readLine();
                if (!reader.ready() || sensorReadingString == null) {
                    reader.close();
                    gzipStream.close();
                    gzipStream = new GZIPInputStream(new FileInputStream(filePath));
                    reader =
                            new BufferedReader(
                                    new InputStreamReader(gzipStream, StandardCharsets.UTF_8));
                    sensorReadingString = reader.readLine();
                }
                sensorRecord = parseSpikeDetectionLine(sensorReadingString);
                if (sensorRecord != null) {
                    sensorRecord.replaceTimestamp(String.valueOf(System.currentTimeMillis()));
                    return sensorRecord;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public DataTuple parseSpikeDetectionLine(String sensorEvent) {
        String[] s = sensorEvent.split(" ");
        if (s.length != 8) {
            return null;
        }
        LinkedHashMap<Class<?>, ArrayList<Object>> dataTupleContent = new LinkedHashMap<>();
        ArrayList<Object> stringContent = new ArrayList<>();
        ArrayList<Object> doubleContent = new ArrayList<>();
        ArrayList<Object> intContent = new ArrayList<>();
        long timestamp;
        try {
            timestamp =
                    LocalDateTime.parse(
                                    s[0] + " " + s[1].split("\\.")[0],
                                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                            .toEpochSecond(ZoneOffset.UTC);
            intContent.add(Integer.parseInt(s[2])); // epoch
            intContent.add(Integer.parseInt(s[3])); // moteid
            doubleContent.add(Double.parseDouble(s[4])); // temperature
            doubleContent.add(Double.parseDouble(s[5])); // humidity
            doubleContent.add(Double.parseDouble(s[6])); // light
            doubleContent.add(Double.parseDouble(s[7])); // voltage
        } catch (Exception e) {
            System.err.println("Wrong format --> Error parsing: " + sensorEvent);
            return null;
        }

        dataTupleContent.put(String.class, stringContent);
        dataTupleContent.put(Double.class, doubleContent);
        dataTupleContent.put(Integer.class, intContent);
        return new DataTuple(dataTupleContent, String.valueOf(timestamp));
    }
}
