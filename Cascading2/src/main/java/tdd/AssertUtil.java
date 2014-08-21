package tdd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import junit.framework.Assert;

public class AssertUtil {

	public static void isContentSame(String expectedFile, String actualFile)  {
		byte[] src;
		try {
			src = Files.readAllBytes(Paths.get(expectedFile));
		
		String srcContent = new  String(src);
		byte[] dest = Files.readAllBytes(Paths.get(actualFile));
		String destContent = new String(dest);
		System.out.println(srcContent);
		System.out.println();
		System.out.println(destContent);
		Assert.assertTrue(srcContent.trim().equalsIgnoreCase(destContent.trim()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			Assert.fail();
		}
	}

	
	
	
}
