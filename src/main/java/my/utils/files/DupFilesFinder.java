package my.utils.files;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.log4j.Logger;


/**
 * This class starts a utility application which should find and report all dup files.<br>
 * 
 * Note: <br>
 * <li>Based on the requirements of the task, I'll skip wrapper based on shell with help section, handling incorrect input params, etc.<br>
 * <li>Assumption is that "100G worth of files" means 100+ billion files, which worse than 100GB total in files, so it will be more interesting.
 * <br><br>
 * <b>Hard links</b> with siblings outside the given dir structure will be ignored and not reported<br>
 * Hard links are independent and therefore will be reported in order of appearance.<br>
 * <b>Soft links</b> to files will be reported (if not required - can be switched off in FileVisitor. Property can be used for controlling this behavior).
 * <b>Soft links</b> to directories will be ignored.
 *  
 * @author Edward Berezitsky
 *
 */
public class DupFilesFinder {
	
	private static Path startingDir = Paths.get(System.getProperty("user.dir"));
	
	private final static Logger log = Logger.getLogger(DupFilesFinder.class);
	
	public final static String TEMP_DIR = System.getProperty("java.io.tmpdir");
	public final static int ERROR_INPUT_VALIDATION = 501;
	public final static String FIELD_DELIMITER = "\\";

	private static final int MIN_COUNT_CHECKSUM = 3;

	private static final int BUFFER_CHECKSUM = 4 * 1024;
	
	private static Checksum adlerChecksum = new Adler32();
	private static FilesContentComparator comparator = new FilesContentComparator();
	

	/** Good Luck with Dup Files. */
	public static void main(String[] args){
		
		parseArgs(args);
		
		Scanner scanner = new Scanner();
		Path sortedFilesList = null;
		try{
			sortedFilesList = scanner.scan(startingDir.toString());
			log.info("Found files: " + (sortedFilesList == null? "none": sortedFilesList.toString()));
		}catch(IOException e){
			log.error("Search incomplete! Exception: ", e);
			System.out.println("ERROR: Search incomplete! Refer to log file for more details.");
		}

		if (sortedFilesList == null){
			return;
		}
		compareFiles(sortedFilesList);
	}


	


	/**
	 * Apache Common CLI package can be used to better handle input params.<br>
	 * 
	 * @param args path to the starting directory
	 * 
	 */
	private static void parseArgs(String[] args) {
		if (args == null || args.length == 0) return;
		try{
			startingDir = Paths.get(args[0]);
		}catch(InvalidPathException e){
			System.out.println("Invalid Path provided: " + args[0]);
			log.error("Invalid Path provided: " + args[0], e);
			System.exit(ERROR_INPUT_VALIDATION);
		}
		
		if (!startingDir.toFile().exists() || !startingDir.toFile().isDirectory()){
			System.out.println("Path doesn't exists: " + args[0]);
			log.error("Path doesn't exists: " + args[0]);
			System.exit(ERROR_INPUT_VALIDATION);
		}
		
		log.info("Scanning for dup files in " + startingDir.toString());
	}

	/**
	 * Given sorted list by keys, compare files (by size, inode info, checksum or content
	 * @param sortedFilesList
	 */
	private static  void compareFiles(Path sortedFilesList) {
		if (!Files.exists(sortedFilesList, LinkOption.NOFOLLOW_LINKS)
				|| !Files.isReadable(sortedFilesList)){
			log.info("This is not real situation, or somebody removed the temp file with sorted files, or access denied...");
			return;
		}
		
		FileCollector fc = new FileCollector(sortedFilesList.getParent().toString());
		int groupCount = 0;
		String line;
		String fileKey, prevFileKey = null;
		String fileName, prevFileName = null;
		String lenStr, prevLen=null;
		int fileNameIndex = 0;
		List<String> groupList = new ArrayList<String>();
		
		log.debug("Reading sorted file with all the files: " + sortedFilesList.toString());
		//filter and report hard links first, and process small groups for which checksum is not required.
		try(BufferedReader reader = Files.newBufferedReader(sortedFilesList, StandardCharsets.UTF_8)){
			while ((line = reader.readLine()) != null){
				fileKey = line.substring(0, (fileNameIndex=line.indexOf(FileCollector.C_DELIMITER)));
				fileName = line.substring(fileNameIndex+1);
				log.debug(String.format("processing: %s ", fileName));
				//if file has the same inode (hard link), not tested on other FS (NTFS, FAT, etc)
				if (fileKey.equals(prevFileKey)){
					log.debug(String.format("Hard Links: %s  =  %s", fileName, prevFileName));
					System.out.println(String.format("Hard Links: %s  =  %s", fileName, prevFileName));
					continue; //we don't need to keep this record anymore, because these two files are the same for sure.
				}
				
				lenStr = fileKey.substring(0, fileKey.indexOf(FIELD_DELIMITER));
				if (prevFileKey == null){ //first line
					log.debug(String.format("First file: %s %s", fileKey, fileName));
					prevLen = lenStr;
					prevFileKey = fileKey;
					prevFileName = fileName;
					groupList.add(fileName);
					groupCount++;
					continue;
				}

				if (lenStr.equals(prevLen)){
					log.debug(String.format("Same length: %s %s, groupCount=%d", prevFileName, fileName, groupCount));
					if (groupCount == MIN_COUNT_CHECKSUM){
						groupList.add(fileName);
						for (String fn: groupList){
							long csum = getCheckSum(adlerChecksum, fn);
							fc.pushFile("" + csum + FIELD_DELIMITER + lenStr, fn);
						}
						groupList.clear();
					}else if (groupCount > MIN_COUNT_CHECKSUM){ //min size group already process, so each next file should be computed for checksum
						long csum = getCheckSum(adlerChecksum, fileName);
						fc.pushFile("" + csum + FIELD_DELIMITER + lenStr, fileName);
					}else{
						groupList.add(fileName);
						log.debug("added to groupList: " + fileName);
					}
					groupCount++;
					prevLen = lenStr;
					prevFileKey = fileKey;
					prevFileName = fileName;
					continue;
				}
				
				if (!lenStr.equals(prevLen)){ //this "if" can be skipped, because if we are here, it is always true. Adding for readability.
					//new group is starting. So need to process the currently collected one.
					if (!groupList.isEmpty()){ //no need to check, either group is small or empty. 
						for (int i=0; i< groupList.size() -1; i++){
							if (groupList.get(i) == null) continue; //skip deleted to avoid cross checks.
							for (int j=i+1; j< groupList.size(); j++){
								if (groupList.get(j) == null) continue; //for the same reason
								log.debug(String.format("Comparing small batch: %s %s", groupList.get(i), groupList.get(j)));
								if (comparator.compare(groupList.get(i), groupList.get(j)) == 0){
									System.out.println(String.format("Dup  Files: %s  =  %s", groupList.get(i), groupList.get(j)));
									groupList.set(j, null); 
								}
							}
						}
					}
					groupList.clear();;
					groupList.add(fileName);
					groupCount=0;
					prevLen = lenStr;
					prevFileKey = fileKey;
					prevFileName = fileName;
					continue;
				}
				
			}
			fc.finish();
			Path sortedChecksums = fc.getSortedFile();
			reportByChecksum(sortedChecksums);
		} catch (IOException e) {
			log.error(String.format("Couldn't read sorted file %s", sortedFilesList.toString()));
			return;
		}
	}

	/**
	 * Runs comparator for similar files, for a given final sorted file with checksums
	 * @param sortedChecksums
	 */
	private static void reportByChecksum(Path sortedChecksums) {
		
		if (sortedChecksums == null) return; //there was nothing collected, probably all files have different size, so no checksum/content test required. 
		
		String line;
		String fileKey, prevFileKey = null;
		String fileName;
		int fileNameIndex = 0;
		List<String> groupList = new ArrayList<String>();

		try(BufferedReader reader = Files.newBufferedReader(sortedChecksums, StandardCharsets.UTF_8)){
			while ((line = reader.readLine()) != null){
				fileKey = line.substring(0, (fileNameIndex=line.indexOf(FileCollector.C_DELIMITER)));
				fileName = line.substring(fileNameIndex+1);
				
				//if file has the same csum and size we need to collect them for cross check
				if (fileKey.equals(prevFileKey)){
					//to avoid collecting (worst case scenario: all files are copies of the same file), identify on the fly, without collecting
					boolean isDup = false;
					for (String fn: groupList){
						if (comparator.compare(fn, fileName) == 0){
							System.out.println(String.format("Dup  Files: %s  =  %s", fn, fileName));
							isDup = true;
							break;
						}
					}
					if (isDup) continue; //don't save as previous file, we found dup and reported it!
					groupList.add(fileName); //possible failure point (out of memory), as if some crazy guy generated DIFFERENT files with the same size and checksum (which is possible), this will fail.
					
					continue; //we don't need to keep this record anymore, because these two files are the same for sure.
				}
				if (prevFileKey == null){ //first line
					prevFileKey = fileKey;
					groupList.add(fileName);
					continue;
				}
				if (!fileKey.equals(prevFileKey)){
					for (int i=0; i< groupList.size() -1; i++){
						if (groupList.get(i) == null) continue; //skip deleted to avoid cross checks.
						for (int j=i+1; j< groupList.size(); j++){
							if (groupList.get(j) == null) continue; //for the same reason
							log.debug(String.format("after csum compare: %s %s", groupList.get(i), groupList.get(j)));
							if (comparator.compare(groupList.get(i), groupList.get(j)) == 0){
								log.debug("It's dup!");
								System.out.println(String.format("Dup  Files: %s  =  %s", groupList.get(i), groupList.get(j)));
								groupList.set(j, null);
							}
						}
					}
					prevFileKey = fileKey;
					groupList.clear(); // BUG
					groupList.add(fileName);
				}

			}
		} catch (IOException e) {
			log.error(String.format("Couldn't read sorted file %s with checksums", sortedChecksums.toString()));
		}
	}


	/**
	 * Computes checksum for given file.
	 * @param adlerChecksum
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	private static long getCheckSum(Checksum checksum, String fileName) throws IOException {
		checksum.reset();
		try(	FileInputStream inputStream = new FileInputStream(fileName);
				CheckedInputStream cinStream = new CheckedInputStream(inputStream, checksum)) {

			byte[] b = new byte[BUFFER_CHECKSUM];
			while (cinStream.read(b) >= 0) {
			}
			return cinStream.getChecksum().getValue();
		} catch (IOException e) {
			throw e;
		}
	}

}
