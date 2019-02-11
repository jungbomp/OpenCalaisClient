package com.calais.client;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;
import java.io.*;

/************************************************************
 * - Simple Calais client to process file or files in a folder - Takes 3
 * arguments 1. File or folder name to process 2. Output folder name to store
 * response from Calais 3. Token - Please specify the correct web service
 * location url for CALAIS_URL variable - Please adjust the values of different
 * request parameters in the createPostMethod
 * 
 **************************************************************/

public class App {

    public static void main(String[] args) throws Exception {

        verifyArgs(args);
        
        String jsonFileName = args[0];
        String csvFileName = args[1];
        String descriptionFileName = args[2];
        String entityFileName = args[3];
        int cnt = Integer.valueOf(args[4]);
        
        HashMap<String, List<Entry<String, String>>> hm = getData(csvFileName);
        List<Entry<String, String>> inputList = getInputDataFromJson(jsonFileName, hm);
        
        // Gelerate random numbers
        Set<Integer> nums = generateRandomNumbers(inputList.size(), cnt, inputList, hm);

        // Save randomly chosen descriptions with key
        saveRandomListToTsv(inputList, nums, descriptionFileName);

        // Save randomly chosen Calais results
        saveRandomResultToCSV(inputList, hm, nums, entityFileName);
    }

    private static void verifyArgs(String[] args) {
        if (0 == args.length) {
            usageError("no params supplied");
        } else if (args.length < 5) {
            usageError("5 params are required");
        } else {
            if (!new File(args[0]).exists())
                usageError("file " + args[0] + " doesn't exist");

            if (!new File(args[1]).exists())
                usageError("file " + args[1] + " doesn't exist");
        }
    }

    private static void usageError(String s) {
        System.err.println(s);
        System.err.println("Usage: java " + (new Object() { }.getClass().getEnclosingClass()).getName() + " jsonFile csvFile descriptionResultFile eitityResultFile count");
        System.exit(-1);
    }
    
    private static List<Entry<String, String>> getInputDataFromJson(final String filepath, HashMap<String, List<Entry<String, String>>> hm) throws Exception {
                    
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
                if (!hm.containsKey(entry.getKey())) {
                    continue;
                }
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
    
    private static Set<Integer> generateRandomNumbers(int boundary, int cnt, List<Entry<String, String>> list, HashMap<String, List<Entry<String, String>>> hm) {

        HashSet<Integer> hs = new HashSet<Integer>();

        Random rand = new Random();
        while (hs.size() < cnt) {
            int num = 0;
            for (num = rand.nextInt(boundary); hs.contains(num); num = rand.nextInt(boundary));

            if (hm.get(list.get(num).getKey()) == null)
                continue;
            hs.add(num);
        }

        return hs;
    }

    private static void saveRandomListToTsv(List<Entry<String, String>> list, Set<Integer> numberSet, String filePathName) {
        PrintWriter writer = null;
        try {
            File out = new File(filePathName);
            writer = new PrintWriter(new BufferedWriter(new FileWriter(out)));
            
            for (Integer i : numberSet) {
                Entry<String, String> entry = list.get(i);
                String key = entry.getKey().replace("\n", "").replace("\t", "");
                String value = entry.getValue().replace("\n", "").replace("\t", "");
                writer.println(key + "\t" + value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) try {writer.close();} catch (Exception ignored) {}
        }
    }

    private static void saveRandomResultToCSV(List<Entry<String, String>> list, HashMap<String, List<Entry<String, String>>> map, Set<Integer> numSet, String filePathName) {
        PrintWriter writer = null;
        try {
            File out = new File(filePathName);
            writer = new PrintWriter(new BufferedWriter(new FileWriter(out)));

            int cnt = 0;
            
            for (Integer i : numSet) {
                String key = list.get(i).getKey();
                List<Entry<String, String>> entities = map.get(key);

                if (entities == null) {
                    cnt++;
                    continue;
                }

                for (Entry<String, String> entity : entities) {
                    String csvStr = key + "," + entity.getKey() + "," + entity.getValue().replace("\n", "");
                    writer.println(csvStr);
                }
            }

            System.out.println("Skipped line : " + cnt);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) try {writer.close();} catch (Exception ignored) {}
        }
    }

    private static HashMap<String, List<Entry<String, String>>> getData(String filepath) throws IOException {

        File inputFile = new File(filepath);
        if (!inputFile.exists()) {
            throw new IOException("Can't find link file from " + filepath);                            
        }

        HashMap<String, List<Entry<String, String>>> hm = new HashMap<String, List<Entry<String, String>>>();
        
        Scanner sc = null;
        try {
            sc = new Scanner(new File(filepath));
            while (sc.hasNextLine()) {
                String[] tokens = sc.nextLine().split(",");
                
                List<Entry<String, String>> list = hm.get(tokens[0]);
                if (null == list) {
                    list = new ArrayList<Entry<String, String>>();
                }

                if (tokens[1].equals("Organization") || tokens[1].equals("Person")) {
                    list.add(new SimpleEntry(tokens[1], tokens[2]));
                    hm.put(tokens[0], list);
                }
            }

		} catch (IOException e) {
            System.err.println(e.getMessage());
			e.printStackTrace();
			throw e;
        } finally {
            if (null != sc) 
                sc.close();
        }

        return hm;
    }
}
