package plangeneratorflink.operators.source;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class AdvertisementSourceOperator extends AbstractOperator<DataGeneratorSource<DataTuple>>
        implements DataGenerator<DataTuple> {

    private final int eventRate;
    private final String downloadPath;
    private String filePath;
    private InputStream inputStream;
    private Constants.AD.SourceType sourceType;
    private BufferedReader reader;

    public AdvertisementSourceOperator(
            String id, String downloadPath, int eventRate, Constants.AD.SourceType sourceType) {
        setId(id);
        this.downloadPath = downloadPath;
        this.eventRate = eventRate;
        this.sourceType = sourceType;
        this.function = new DataGeneratorSource<>(this, eventRate, null, getDescription());
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put("operatorType", "SourceOperator");
        description.put("confEventRate", eventRate);
        description.put("numInteger", 1);
        description.put("numDouble", 0);
        description.put("numString", 2);
        description.put("tupleWidthOut", 3 + 1); // add timestamp value manually
        return description;
    }

    @Override
    public void open(
            String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
            throws Exception {

        String dir = System.getProperty("user.dir");
        String fileName = "ad-clicks.dat";
        filePath = dir + "/" + fileName;
        File file = new File(filePath);
        if (file.exists()) {
            System.out.println(
                    "No need to download again, file ad-clicks.dat already exists at " + filePath);
        } else {
            try {
                URL url = new URL(downloadPath);
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
                                    + downloadPath
                                    + "): "
                                    + httpConn.getResponseCode());
                }
                httpConn.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Downloaded ad-clicks.dat to " + filePath);
        }

        inputStream = new FileInputStream(filePath);
        reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public DataTuple next() {
        String recordReadingString = "";
        try {
            recordReadingString = reader.readLine();
            if (reader == null || !reader.ready() || recordReadingString == null) {
                if (reader != null) {
                    reader.close();
                    inputStream.close();
                }
                inputStream = new FileInputStream(filePath);
                reader =
                        new BufferedReader(
                                new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                recordReadingString = reader.readLine();
            }
            DataTuple record = parseAdvertisementLine(recordReadingString);
            if (record != null) {
                return record;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public DataTuple parseAdvertisementLine(String event) {
        String[] s = event.split("\t");
        if (s.length != 12) {
            System.err.println("read advertisement line: length of line is not 12");
            return null;
        }
        LinkedHashMap<Class<?>, ArrayList<Object>> dataTupleContent = new LinkedHashMap<>();
        ArrayList<Object> stringContent = new ArrayList<>();
        ArrayList<Object> doubleContent = new ArrayList<>();
        ArrayList<Object> intContent = new ArrayList<>();
        try {
            stringContent.add(s[0]); // queryId
            stringContent.add(s[1]); // adId
            if (this.sourceType.equals(Constants.AD.SourceType.impressions)) {
                intContent.add(Integer.parseInt(s[4])); // clicks
            } else if (this.sourceType.equals(Constants.AD.SourceType.clicks)) {
                intContent.add(Integer.parseInt(s[5])); // impressions
            }

        } catch (Exception e) {
            System.err.println("Wrong format --> Error parsing: " + event + ". Error: " + e);
            return null;
        }

        dataTupleContent.put(String.class, stringContent);
        dataTupleContent.put(Double.class, doubleContent);
        dataTupleContent.put(Integer.class, intContent);
        // dataTuple structure: timestamp, queryId (String), adId (String), clicks/impressions
        // (Integer)
        return new DataTuple(dataTupleContent, String.valueOf(System.currentTimeMillis()));
    }
}
