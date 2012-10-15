package org.apache.hive.util;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HiveRCGeneratorTest {

	private HiveRCGenerator generator;

	@Before
	public void setUp() {
		generator = new HiveRCGenerator();
	}

	@After
	public void tearDown() {
	}

	@Description(name = "udf_func", value = "description")
	private static class TestUDFClass extends UDF {

	}

	private static class ClassWithoutUdfAnnotation {

	}

	@Test
	public void shouldReturnRCStringGivenUDFClass() {
		String rcStr = generator.generate(TestUDFClass.class);
		assertEquals("CREATE TEMPORARY FUNCTION UDF_FUNC AS '"
				+ TestUDFClass.class.getName() + "';", rcStr);
	}

	@Test
	public void shouldReturnEmptyGivenClassWithoutUdfAnotation() {
		String rcStr = generator.generate(ClassWithoutUdfAnnotation.class);
		assertTrue(rcStr.isEmpty());
	}

	@Test
	public void shouldReturnEmptyGivenClassIsNull() {
		String rcStr = generator.generate(null);
		assertTrue(rcStr.isEmpty());
	}

	@Test(expected = FileNotFoundException.class)
	public void shouldThrowFileNotExistExceptioinGivenNonExistJar()
			throws IOException {
		generator.generate("non-exists.jar", new OutputStreamWriter(
				new ByteArrayOutputStream()));
	}

	@Test
	public void shouldOutputCreateFunctionsGivenJarFile() throws IOException,
			InterruptedException {
		String[] udfs = new String[] { "udf1_func", "udf2_func" };
		String packageName = "org.apache.hive.udfs";

		String jarFile = createJarWithHiveUdfs(packageName, udfs);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		generator.generate(jarFile, new OutputStreamWriter(baos));

		baos.close();

		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < udfs.length; i++) {
			builder.append("CREATE TEMPORARY FUNCTION ")
					.append(udfs[i].toUpperCase()).append(" AS '")
					.append(packageName).append(".MyUdf").append(i + 1)
					.append("';\n");
		}

		assertEquals(builder.toString(), baos.toString());
	}

	private String createJarWithHiveUdfs(String packageName, String[] udfs)
			throws IOException, InterruptedException {
		Process proc = Runtime.getRuntime().exec(
				"mvn -f target/test-classes/test-udfs/pom.xml clean package");
		proc.waitFor();
		if (proc.exitValue() != 0) {
			throw new RuntimeException("Failed to build test package!");
		}
		return "target/test-classes/test-udfs/target/test-udfs-0.0.1-SNAPSHOT.jar";
	}
}
