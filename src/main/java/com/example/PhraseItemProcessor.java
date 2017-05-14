package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.gson.Gson;

public class PhraseItemProcessor implements ItemProcessor<String, List<PhraseCount>> {

	@Autowired
	JdbcTemplate jdbcTemplate;

	private static final Logger log = LoggerFactory.getLogger(PhraseItemProcessor.class);
	private Gson gson = new Gson();

	@Override
	public List<PhraseCount> process(final String str) throws Exception {
		System.out.println(str);
		String[] keywords = str.split("\\|");
		List<PhraseCount> phraseCounts = new ArrayList<PhraseCount>();
		for (int i = 0; i < keywords.length; i++) {
			String keyword = keywords[i].trim();
			PhraseCount pc = new PhraseCount(keyword, 1);
			phraseCounts.add(pc);
			getCount(keyword);
		}

		return phraseCounts;
	}

	public void getCount(String phrase) throws IOException {

		List<Integer> counts = jdbcTemplate.queryForList("SELECT phrase_count FROM phrase_count where phrase = ?",
				int.class, phrase);
		if (counts.iterator().hasNext()) {
			Integer count = counts.iterator().next();
			count++;
			jdbcTemplate.update("update phrase_count set phrase_count= ? where phrase = ? ", count, phrase);
		} else {
			jdbcTemplate.update("INSERT INTO phrase_count (phrase, phrase_count) VALUES (?, ?)", phrase, 1);
		}

		// return count;
		/*
		 * InputStream in = new FileInputStream("D:\\output.txt"); JsonReader
		 * reader = new JsonReader(new InputStreamReader(in, "UTF-8"));
		 * reader.beginArray(); while (reader.hasNext()) { PhraseCount
		 * phraseCount = gson.fromJson(reader, PhraseCount.class);
		 * if(phraseCount.equals(phrase)){ return phraseCount.getCount(); } }
		 * reader.endArray(); reader.close();
		 */

		/*
		 * FileInputStream inputStream = null; Scanner sc = null; try {
		 * inputStream = new FileInputStream("D:\\output.txt"); sc = new
		 * Scanner(inputStream, "UTF-8"); while (sc.hasNextLine()) { String line
		 * = sc.nextLine();
		 * 
		 * PhraseCount phraseCount = gson.fromJson(line, PhraseCount.class);
		 * if(phrase.equals(phraseCount.getPhrase())){ return
		 * phraseCount.getCount(); } }
		 * 
		 * } finally { if (inputStream != null) { inputStream.close(); } if (sc
		 * != null) { sc.close(); } } return 0;
		 */
	}

}
