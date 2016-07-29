package my.utils.files;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import org.apache.log4j.Logger;

/**
 * Compares content of two files.
 * @author Ed Berezitsky
 *
 */
public class FilesContentComparator implements Comparator<String> {

	/** This buffer size can be tuned depending on stats of files size */
	private static final int BUFFER_SIZE = 8 * 1024;
	
	private final static Logger log = Logger.getLogger(FilesContentComparator.class);
	
	
	/**
	 * Compares content of two given files.
	 * @param filePath1 the first file to be compared.
	 * @param filePath2 the second file to be compared.
	 * 
	 * @return 
	 * <li>a negative number if first file doesn't exist, a size of first file less than second one, or first different byte in first file less than in second.
	 * <li>zero if files are absolutely identical
	 * <li>a positive number if second file doesn't exist, or both previous statements are not true.
	 */
	@Override public int compare(String filePath1, String filePath2) {
		Path p1 = Paths.get(filePath1);
		Path p2 = Paths.get(filePath2);
		
		File f1 = p1.toFile();
		File f2 = p2.toFile();
		
		if (!f1.exists()) return -1;
		if (!f2.exists()) return 1;
		
		int result;
		if ((result=(Long.compareUnsigned(f1.length(), f2.length()))) != 0) return result;
		
		try(	FileInputStream fis1 = new FileInputStream(f1);
				BufferedInputStream bis1=new BufferedInputStream(fis1, BUFFER_SIZE);
				FileInputStream fis2 = new FileInputStream(f2);
				BufferedInputStream bis2=new BufferedInputStream(fis2, BUFFER_SIZE)){
			
			int b1 = 0, b2 = 0;
			while(result == 0 && b1 >= 0){
				b1 = bis1.read();
				b2 = bis2.read();
				result = Integer.compare(b1, b2);
			}
		} catch (IOException e) {
			//suppress IO Exception (on closed Input streams) and IO errors. This shouldn't happen.
			log.error(String.format("Couldn't compare files %s and %s", filePath1, filePath2), e);
			return -1;
		} 
				
		return result;
	}
}
