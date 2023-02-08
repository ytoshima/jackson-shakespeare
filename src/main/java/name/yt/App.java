package name.yt;

import java.io.IOException;
import java.io.File;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.SequenceWriter;

/**
 * App: convert ndjson for ElasticSearch 5.x to 7.x.
 * 5.x format may have multiple type for an index.  This program creates
 * JSON file for each '_index' + '-' + '_type' combination.
 */
public class App {
    //static final String JS1 = "/Users/yoshi/Documents/work/kibana/mini-ex/mini-shakespeare.json";
    static final String JS1 = "mini-shakespeare.json";
    static final Map<String,SequenceWriter> seqwMap = new TreeMap<>();
    public static void main( String[] args ) throws Exception {
        File input = getInputFile(args);
        ObjectMapper om = new ObjectMapper();
	if (input == null) {
            System.out.println("I: Trying default path " + JS1);
	    input = Paths.get(JS1).toFile();
	}
        try (MappingIterator<JsonNode> it = om.readerFor(JsonNode.class).readValues(input)) {
            while (it.hasNextValue()) {
              // index line before data, e.g. {"index":{"_index":"mini-shakespeare","_type":"act","_id":0}}
	      JsonNode node = it.nextValue(); 
	      // index value
	      JsonNode idx  = node.get("index");
	      // Create new data index from _index and _type
	      JsonNode dind = node.at("/index/_index");
	      JsonNode typ  = node.at("/index/_type");
	      String dIndStr = "_na_";
	      String typeStr = "_na_";
	      if (dind != null) dIndStr = dind.asText();
	      if (typ != null) typeStr = typ.asText();
	      String synthIndStr = dIndStr + "-" + typeStr;

	      if (idx != null) {
                final boolean debug = false;
		if (debug) {
	          System.out.println("node: " + node);
	          System.out.println("idx: " + idx);
		}

		ObjectNode nIdx = ((ObjectNode)idx).put("_index", synthIndStr);
		nIdx.remove("_type");
		if (debug) {
	          System.out.println("nIdx: " + nIdx);
		}
		ObjectNode nNode = ((ObjectNode)node);
		nNode.replace("index", nIdx);

		if (debug) {
		  System.out.println("nNode: " + nNode);

	          System.out.println(synthIndStr);
		}
		JsonNode bodyJn = it.nextValue();
		if (debug) {
	          System.out.println("  " + bodyJn);
		}

		SequenceWriter sw = getSeqWriterForIndex(synthIndStr);
		sw.write(nNode);
		sw.write(bodyJn);

	      }
	    }
	}
        closeSeqWriters();
    }
    /**
     * Returns SequenceWriter instance for given index String from the seqwMap
     * creating new SequenceWriter instance as needed.
     */
    static SequenceWriter getSeqWriterForIndex(String indexStr) throws IOException {
      SequenceWriter sw = seqwMap.get(indexStr);
      if (sw == null) {
        File f = new File(indexStr + ".json");
        if (!f.exists()) {
          f.createNewFile();	
	} 	
        sw = new ObjectMapper().writer().withRootValueSeparator("\n").writeValues(f);
	seqwMap.put(indexStr, sw);
      }      
      return sw;
    }

    /**
     * Close SequenceWriter instances in seqwMap
     */
    static void closeSeqWriters() throws IOException {
      for (SequenceWriter sw : seqwMap.values()) {
        sw.close(); 
      } 
    }

    /**
     * Returns a File whose path is args[0] if args[0] exists.  
     * If the path is invalid or args[0] does not exist, returns null.
     */
    static File getInputFile(String[] args) {
      File f = null;
      if (args.length > 0) {
         f = new File(args[0]);
	 if (!f.exists()) {
           System.out.println("E: file " + args[0] + " does not exist.");
  	   f = null;
	 }
      }  
      return f;
    }
}
