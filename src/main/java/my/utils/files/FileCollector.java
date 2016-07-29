package my.utils.files;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;


/**
 * FileCollector collects files and keep them sorted.
 * Once LIMIT reached, it will flashes into tmp location.
 * 
 * This class can be improved by creating multiple files for flushed sorted maps.
 * This way merge sort can be optimized and executed in parallel.
 * @author Edward Berezitsky
 *
 */
public class FileCollector{

	public static final String S_DELIMITER = ":";
	public static final char C_DELIMITER = S_DELIMITER.charAt(0);

	/* temp location of sorted files */
	private String tmpLoc;
	private Path fullFile;
	
	private static final int MAX_MAP_SIZE = 100000;
	private static final String EOL = System.getProperty("line.separator");
	
	private Map<String, List<String>> sortedFiles = new TreeMap<String, List<String>>();
	private long mapSize = 0;
	
	
	private final static Logger log = Logger.getLogger(FileCollector.class);
	
	public FileCollector(String tempFilesLocation) {
		tmpLoc = tempFilesLocation;
	}
	
	/**
	 * Push file into sorted collector.
	 * All files will be sorted by provided key.
	 * @param fileKey
	 * @param fileName
	 * @return
	 * @throws IOException 
	 */
	public void pushFile(String fileKey, String fileName) throws IOException{
		log.debug("collecting file: " + fileName + " with key: " + fileKey);
		if (mapSize >= MAX_MAP_SIZE){
			flushMap(sortedFiles);
			sortedFiles.clear();
			mapSize = 0;
		}
		List<String> l = sortedFiles.get(fileKey);
		if (l == null){
			l = new LinkedList<String>();
			sortedFiles.put(fileKey, l);
		}
		l.add(fileName);
		mapSize++;
	}
	
	/**
	 * Finishes collecting files, flush currently collected items.
	 * @throws IOException 
	 */
	public Path finish() throws IOException{
		log.debug("finishing ...");
		flushMap(sortedFiles);
		return fullFile;
	}
	
	/**
	 * Returns the file with all sorted elements
	 * @return
	 */
	public Path getSortedFile(){
		return fullFile;
	}

	/**
	 * Flushes sorted map of files into file while keeping the file sorted.
	 * Performance can be improved by flushing in separate thread.
	 * @param filesMap
	 * @throws IOException 
	 */
	private void flushMap(Map<String, List<String>> filesMap) throws IOException {
		
		if (filesMap == null || filesMap.isEmpty()){
			log.debug("there is nothing to flash...");
			return;
		}
		
		if (fullFile == null){ //first time execution, flush directly into new file
			try {
				fullFile = Files.createTempFile(Paths.get(tmpLoc), "sortedFiles", ".tmp");
			} catch (IOException e) {
				log.error("Cannot create new files. Make sure there is available space and required permissions.", e);
				throw e;
			}
			log.debug("temp file for sorted files: " + fullFile.toString());
			try( BufferedWriter writer = Files.newBufferedWriter(fullFile, StandardCharsets.UTF_8, StandardOpenOption.WRITE)){
				for(Entry<String, List<String>> entry : filesMap.entrySet()){
					for (String fileName : entry.getValue()){ //flatten list
						writer.write(entry.getKey());
						writer.write(S_DELIMITER);
						writer.write(fileName);
						writer.write(EOL);
					}
				}
			}catch(IOException e){
				log.error("Cannot write into file. Make sure there is available space.", e);
				log.info(String.format("Temp file %s will be deleted. Size: %d ", fullFile.toString(), fullFile.toFile().length()));
				try{
					Files.delete(fullFile);
				}catch(Exception ee){
					log.error(String.format("Couldn't delete temp file %s, manual actions required!", fullFile.toString()));
				}
			}
		}else{ // need to flush and to merge with existing sorted file, after assign fullFile to newly created one.
			Path newFullFile = null;
			try {
				newFullFile = Files.createTempFile(Paths.get(tmpLoc), "sortedFiles", ".tmp");
			} catch (IOException e) {
				log.error("Cannot create new files. Make sure there is available space.", e);
				throw e;
			}
			try( BufferedWriter writer = Files.newBufferedWriter(newFullFile, StandardCharsets.UTF_8, StandardOpenOption.WRITE);
					BufferedReader reader = Files.newBufferedReader(fullFile, StandardCharsets.UTF_8)){
				String line, fileKeyMap, fileKeyFile;
				Iterator<Entry<String, List<String>>> iter = filesMap.entrySet().iterator();
				Entry<String, List<String>> entry;

				entry = !iter.hasNext()? null : iter.next();
				fileKeyMap = entry == null? null: entry.getKey();
				
				line = reader.readLine();
				fileKeyFile = (line == null)? null: line.substring(0, line.indexOf(C_DELIMITER));

				while(true){
					// if finished both file and map
					if (fileKeyMap == null && fileKeyFile == null) break; 
					
					// if both are still having data
					if (fileKeyMap != null && fileKeyFile != null){
						if (fileKeyMap.compareTo(fileKeyFile)>=0){
							writer.write(line);
							writer.write(EOL);
							//now get new line from input file
							line = reader.readLine();
							fileKeyFile = (line == null)? null: line.substring(0, line.indexOf(C_DELIMITER));
							continue;
						}else{
							for(String fn: entry.getValue()){
								writer.write(fileKeyMap);
								writer.write(S_DELIMITER);
								writer.write(fn);
								writer.write(EOL);
							}
							//now get new entry from iterator
							entry =!iter.hasNext()? null : iter.next();
							fileKeyMap = entry == null? null: entry.getKey();
							continue;
						}
					}
					
					// if a map is finished
					if (fileKeyMap == null){
						writer.write(line);
						writer.write(EOL);
						//now get new line from input file
						line = reader.readLine();
						fileKeyFile = (line == null)? null: line.substring(0, line.indexOf(C_DELIMITER));
						continue;
					}
					
					// if a file is finished
					if (fileKeyFile == null){
						for(String fn: entry.getValue()){
							writer.write(fileKeyMap);
							writer.write(S_DELIMITER);
							writer.write(fn);
							writer.write(EOL);
						}
						//now get new entry from iterator
						entry =!iter.hasNext()? null : iter.next();
						fileKeyMap = entry == null? null: entry.getKey();
						continue;
					}
				}
			}
			Files.delete(fullFile);
			fullFile = newFullFile;
		}
	}
}
