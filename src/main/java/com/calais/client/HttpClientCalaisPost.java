package com.calais.client;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;
import java.io.*;

/************************************************************
 * This is implementation of Open Calais Intelligent Tagging RESTfull API
 * - Simple Calais client to process file or files in a folder - Takes 3
 * arguments 1. File or folder name to process 2. Output folder name to store
 * response from Calais 3. Token - Please specify the correct web service
 * location url for CALAIS_URL variable - Please adjust the values of different
 * request parameters in the createPostMethod
 **************************************************************/

public class HttpClientCalaisPost {

    private static final String CALAIS_URL = "https://api.thomsonreuters.com/permid/calais";
    private static final String DEFAULT_UNIQUE_ACCESS_KEY = "";
    private static final String XML_RDF = "xml/rdf";
    private static final String APPLICATION_JSON = "application/json";

    private String uniqueAccessKey;

    private String input;
    private File inputFile;
    private HttpClient client;
    private RequestEntity reqEntity;

    public void setInput(String input) {
        this.input = input;
        try {
            this.reqEntity = new StringRequestEntity(input, "text/plain", "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void setInput(File file) {
        this.inputFile = file;
        this.reqEntity = new FileRequestEntity(file, null);
    }

    public void setUniqueAccessKey(String key) {
        this.uniqueAccessKey = key;
    }

    public void createHttpClient() {
        this.client = new HttpClient();
        this.client.getParams().setParameter("http.useragent", "Calais Rest Client");
    }

    private PostMethod createPostMethod(String retMode) {

        PostMethod method = new PostMethod(CALAIS_URL);

        // Set mandatory parameters
        if (null != uniqueAccessKey && 0 < uniqueAccessKey.length()) {
            method.setRequestHeader("X-AG-Access-Token", uniqueAccessKey);
        } else {
            method.setRequestHeader("X-AG-Access-Token", DEFAULT_UNIQUE_ACCESS_KEY);
        }

        method.setRequestHeader("Content-Type", "text/raw");

		// Set response/output format
        method.setRequestHeader("outputformat", retMode);


        return method;
    }

	public String run() throws Exception {
        return post(createPostMethod(APPLICATION_JSON), reqEntity);
	}

    private String doRequest(PostMethod method) throws Exception {
        try {
            while (true) {
                int returnCode = client.executeMethod(method);
                if (returnCode == HttpStatus.SC_NOT_IMPLEMENTED) {
                    System.err.println("The Post method is not implemented by this URI");
                    // still consume the response body
                    method.getResponseBodyAsString();
                    throw new Exception("The Post method is not implemented by this URI");
                } else if (returnCode != HttpStatus.SC_OK) {
                    System.err.println("Post failed");
                    System.err.println("Got code: " + returnCode);
                    System.err.println("response: " + method.getResponseBodyAsString());

                    if (429 == returnCode) {
                        Thread.sleep(500);
                        continue;
                    }

                    throw new Exception(method.getResponseBodyAsString());
                }

                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            method.releaseConnection();
            throw e;
        } finally {
            
        }

        StringBuilder sb = new StringBuilder();
        System.out.println("Post succeeded");

        BufferedReader reader = new BufferedReader(new InputStreamReader(method.getResponseBodyAsStream(), "UTF-8"));
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }

        method.releaseConnection();

        return sb.toString();
    }

    private String post(PostMethod method, RequestEntity reqEntity) throws Exception {
        method.setRequestEntity(reqEntity);
        return doRequest(method);
    }

    public static void main(String[] args) throws Exception {

        verifyArgs(args);
        
        String inputFileName = args[0];
        String outputFileName = args[1];
        String accessKey = args[2];

        List<Entry<String, List<Entry<String, String>>>> retList = new ArrayList<Entry<String, List<Entry<String, String>>>>();

        List<Entry<String, String>> inputList = getInputDataFromJson(inputFileName);
        HttpClientCalaisPost httpClientPost = new HttpClientCalaisPost();
        httpClientPost.setUniqueAccessKey(accessKey);
        httpClientPost.createHttpClient();
        for (Entry<String, String> entry : inputList) {
            System.out.println("Key : " + entry.getKey());
            httpClientPost.setInput(entry.getValue());
            try {
                String jsonStr = httpClientPost.run();

                retList.add(new SimpleEntry<String, List<Entry<String, String>>>(entry.getKey(), extractTypeEntity(jsonStr)));
                System.out.println("Succeed");

                Thread.sleep(200);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Save Calais result
        saveResultToCSV(retList, outputFileName);
    }

    private static void verifyArgs(String[] args) {
        if (0 == args.length) {
            usageError("no params supplied");
        } else if (args.length < 3) {
            usageError("3 params are required");
        } else {
            if (!new File(args[0]).exists())
                usageError("file " + args[0] + " doesn't exist");
        }
    }

    private static void usageError(String s) {
        System.err.println(s);
        System.err.println("Usage: java " + (new Object() { }.getClass().getEnclosingClass()).getName() + " inputfile accessKey");
        System.exit(-1);
    }
    
    private static List<Entry<String, String>> getInputDataFromJson(final String filepath) throws Exception {
                    
        File inputFile = new File(filepath);
        if (!inputFile.exists()) {
            throw new IOException("Can't find link file from " + filepath);                            
        }

        List<Entry<String, String>> retList = new ArrayList<Entry<String, String>>();

        try {
    		FileInputStream fis;
			fis = new FileInputStream(inputFile);
            BufferedInputStream bis = new BufferedInputStream(fis);
            JsonReader reader = new JsonReader(new InputStreamReader(bis, "UTF-8"));
     
            JsonParser parser = new JsonParser();
            JsonElement element = parser.parse(reader);
            JsonObject json = element.getAsJsonObject();
            Set<Entry<String, JsonElement>> entries = json.entrySet();
            for (Entry<String, JsonElement> entry : entries) {
                JsonObject videoItem = entry.getValue().getAsJsonObject();
                String title = videoItem.get("title").getAsString();
                String description = videoItem.get("description").getAsString();
                retList.add(new SimpleEntry(entry.getKey(), String.format("%s - %s", title, description)));
            }
		} catch (IOException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			throw e;
        }

        return retList;
    }

    private static void saveResultToCSV(List<Entry<String, List<Entry<String, String>>>> list, String filePathName) {
        PrintWriter writer = null;
        try {
            File out = new File(filePathName);
            writer = new PrintWriter(new BufferedWriter(new FileWriter(out)));
            
            for (Entry<String, List<Entry<String, String>>> entry : list) {
                for (Entry<String, String> entity : entry.getValue()) {
                    String csvStr = entry.getKey() + "," + entity.getKey() + "," + entity.getValue().replace("\n", "");
                    System.out.println(csvStr);
                    writer.println(csvStr);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) try {writer.close();} catch (Exception ignored) {}
        }
    }

    private static List<Entry<String, String>> extractTypeEntity(final String jsonStr) throws Exception {   
        
        List<Entry<String, String>> retList = new ArrayList<Entry<String, String>>();

        try {
    		JsonParser parser = new JsonParser();
            JsonElement element = parser.parse(jsonStr);
            JsonObject json = element.getAsJsonObject();
            Set<Entry<String, JsonElement>> entries = json.entrySet();
            for (Entry<String, JsonElement> entry : entries) {
                JsonObject entity = entry.getValue().getAsJsonObject();
                JsonElement typeEntity = entity.get("_type");
                JsonElement nameEntity = entity.get("name");
                if (null == typeEntity || null == nameEntity) continue;

                retList.add(new SimpleEntry<String, String>(typeEntity.getAsString(), nameEntity.getAsString()));
            }
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			throw e;
        }

        return retList;
    }
}
