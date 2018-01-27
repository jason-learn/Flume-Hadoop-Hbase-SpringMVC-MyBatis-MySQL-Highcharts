import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.jason.transformer.model.dim.base.EventDimension;
import com.jason.transformer.service.rpc.IDimensionConverter;
import com.jason.transformer.service.rpc.client.DimensionConverterClient;
import com.jason.transformer.service.rpc.server.DimensionConverterServer;

public class TestRPC {
    public static void main(String[] args) throws IOException {
        DimensionConverterServer.main(args);

        IDimensionConverter converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        System.out.println(converter);
        System.out.println(converter.getDimensionIdByValue(new EventDimension("category", "action")));
        DimensionConverterClient.stopDimensionConverterProxy(converter);
    }
}
