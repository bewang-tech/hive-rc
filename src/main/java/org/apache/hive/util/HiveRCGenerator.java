package org.apache.hive.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

public class HiveRCGenerator {

	public void generate(String jarFilePath, Writer writer) throws IOException {
		File jarFile = new File(jarFilePath);
		if (!jarFile.exists()) {
			throw new FileNotFoundException(jarFilePath + " does not exist.");
		}

		URL[] urls = new URL[] { jarFile.toURI().toURL() };
		URLClassLoader loader = new URLClassLoader(urls);

		Reflections reflections = new Reflections(new ConfigurationBuilder()
				.addUrls(urls).addClassLoader(loader));
		List<Class<?>> udfs = new ArrayList<Class<?>>(
				reflections.getTypesAnnotatedWith(Description.class));
		Collections.sort(udfs, new Comparator<Class<?>>() {
			public int compare(Class<?> c1, Class<?> c2) {
				return c1.getName().compareTo(c2.getName());
			}
		});

		for (Class<?> udf : udfs) {
			writer.write(generate(udf));
			writer.append('\n');
		}
		writer.flush();
	}

	public String generate(Class<?> udfClass) {
		StringBuilder builder = new StringBuilder();
		if (udfClass != null) {
			Description desc = udfClass.getAnnotation(Description.class);
			if (desc != null) {
				builder.append("CREATE TEMPORARY FUNCTION ")
						.append(desc.name().toUpperCase()).append(" AS '")
						.append(udfClass.getName()).append("';");
			}
		}
		return builder.toString();
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			printUsage();
			return;
		}

		try {
			OutputStreamWriter os = new OutputStreamWriter(System.out);
			HiveRCGenerator generator = new HiveRCGenerator();
			generator.generate(args[0], os);
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static void printUsage() {
		System.err.println("Usage: " + HiveRCGenerator.class.getName()
				+ " <jar_file> <output_file>");

	}
}
