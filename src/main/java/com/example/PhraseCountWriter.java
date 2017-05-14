package com.example;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public class PhraseCountWriter  implements ItemWriter<List<PhraseCount>> {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Override
	public void write(List<? extends List<PhraseCount>> items) throws Exception {
		for (List<PhraseCount> phraseCounts : items) {
			for (PhraseCount phraseCount : phraseCounts) {
				List<Integer> counts = jdbcTemplate.queryForList("select phrase_count from phrase_count where phrase = ?",
						int.class, phraseCount.getPhrase());
				if (counts.iterator().hasNext()) {
					Integer count = counts.iterator().next();
					count++;
					jdbcTemplate.update("update phrase_count set phrase_count= ? where phrase = ? ", count, phraseCount.getPhrase());
				} else {
					jdbcTemplate.update("insert into phrase_count (phrase, phrase_count) values (?, ?)", phraseCount.getPhrase(), 1);
				}
			}
		}
		
	}

}
