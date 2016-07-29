package my.utils.files;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.log4j.Logger;

/**
 * Scans all the dirs by running walkFileTree.
 * 
 * @author Edward Berezitsly
 *
 */
public class Scanner {

	private final static Logger log = Logger.getLogger(Scanner.class);
	private final static String DEFAULT_INODE_ID = "()";
	
	
	
	public Scanner(){
	}
	
	/**
	 * Starts scanning directories to get information about files.
	 * 
	 * @param startingDir directory to start scanning files
	 * @return path to the file with all found files sorted by size and inode information
	 * @throws IOException 
	 */
	public Path scan(String startingDir) throws IOException{
		
		log.debug("In scanner");
		Path p = Paths.get(startingDir);
		if (!p.toFile().exists()){
			throw new FileNotFoundException(String.format("%s doesn't exist", p.toString()));
		}
		
		FileCollector fc = new FileCollector(DupFilesFinder.TEMP_DIR);
		log.debug("Collector created at: " + DupFilesFinder.TEMP_DIR);
		try {
			Files.walkFileTree(p, new MyFileVisitor(fc));
		} catch (IOException e) {
			log.error("Couldn't finish walking down the tree...", e);
			throw e;
		}
		
		fc.finish();
		return fc.getSortedFile();
	}
	

	/**
	 * Processes a file from files stream, creates length\filekey, filename pairs and pushes into file collector to sort by length\filekey
	 *
	 */
	class MyFileVisitor extends SimpleFileVisitor<Path>{
		
		private FileCollector fileCollector;
		
		MyFileVisitor(FileCollector fileCollector){
			this.fileCollector = fileCollector;
		}
		
		@Override
		public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs)
				throws IOException {
			
			//log.debug("Found file: " + filePath.toString());
			File file = filePath.toFile();
			//ignore the files that cannot be read and compared anyway...
			if (!file.canRead()) return FileVisitResult.CONTINUE;
			long len = file.length();
			//ignore empty files, they aren't duplicate by definition.
			if (len == 0) return FileVisitResult.CONTINUE;
			
			//get unique ID of the file to find hard links.
			//instead of parsing every such file keys to device id and inode id, use entire string as is.
			Object fk = attrs.fileKey();
			String inode = (fk == null ? DEFAULT_INODE_ID : fk.toString());
			
			String filekey = "" + len + DupFilesFinder.FIELD_DELIMITER + inode;
			
			fileCollector.pushFile(filekey, filePath.toString());
			
			return FileVisitResult.CONTINUE;
		}
	}
}
