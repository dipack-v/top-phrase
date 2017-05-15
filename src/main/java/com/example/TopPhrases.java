package com.example;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.google.gson.Gson;

@SpringBootApplication
public class TopPhrases {

	public static void main(String[] args) {
		SpringApplication.run(TopPhrases.class, args);
	}
}

@Configuration
@EnableBatchProcessing
class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Bean
	public FlatFileItemReader<String> reader() {
		FlatFileItemReader<String> reader = new FlatFileItemReader<String>();
		reader.setResource(new FileSystemResource("keywords.txt"));
		reader.setLineMapper(new PassThroughLineMapper());
		return reader;
	}

	@Bean
	public PhraseItemProcessor processor() {
		return new PhraseItemProcessor();
	}

	@Bean
	public PhraseCountWriter writer() {
		return new PhraseCountWriter();
	}

	// tag::jobstep[]
	@Bean
	public Job topPhraseJob() {
		return jobBuilderFactory.get("topPhraseJob").incrementer(new RunIdIncrementer()).flow(step1()).next(step2())
				.end().build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<String, List<PhraseCount>>chunk(10).faultTolerant()
				.skip(ItemStreamException.class).reader(reader()).processor(processor()).writer(writer()).build();
	}

	@Bean
	public Step step2() {
		return stepBuilderFactory.get("step2").<PhraseCount, PhraseCount>chunk(10).faultTolerant()
				.skip(ItemStreamException.class).reader(databaseItemReader(dataSource)).writer(countWriter()).build();
	}
	// end::jobstep[]

	@Bean
	ItemReader<PhraseCount> databaseItemReader(DataSource dataSource) {
		JdbcCursorItemReader<PhraseCount> databaseReader = new JdbcCursorItemReader<>();
		databaseReader.setDataSource(dataSource);
		databaseReader.setSql("SELECT phrase, phrase_count FROM phrase_count order by phrase_count desc limit 100000 ");
		databaseReader.setRowMapper(new RowMapper<PhraseCount>() {
			@Override
			public PhraseCount mapRow(ResultSet rs, int row) throws SQLException {
				return new PhraseCount(rs.getString(1), rs.getInt(2));
			}
		});

		return databaseReader;
	}

	@Bean
	public FlatFileItemWriter<PhraseCount> countWriter() {
		FlatFileItemWriter<PhraseCount> writer = new FlatFileItemWriter<PhraseCount>();
		writer.setResource(new FileSystemResource("output.txt"));
		writer.setLineAggregator(new PassThroughLineAggregator<>());
		return writer;
	}
}

class PhraseItemProcessor implements ItemProcessor<String, List<PhraseCount>> {

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Override
	public List<PhraseCount> process(final String str) throws Exception {
		System.out.println(str);
		String[] keywords = str.split("\\|");
		List<PhraseCount> phraseCounts = new ArrayList<PhraseCount>();
		for (int i = 0; i < keywords.length; i++) {
			String keyword = keywords[i].trim();
			PhraseCount pc = new PhraseCount(keyword, 1);
			phraseCounts.add(pc);
		}

		return phraseCounts;
	}

}

class PhraseCountWriter implements ItemWriter<List<PhraseCount>> {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public void write(List<? extends List<PhraseCount>> items) throws Exception {
		for (List<PhraseCount> phraseCounts : items) {
			for (PhraseCount phraseCount : phraseCounts) {
				List<Integer> counts = jdbcTemplate.queryForList(
						"select phrase_count from phrase_count where phrase = ?", int.class, phraseCount.getPhrase());
				if (counts.iterator().hasNext()) {
					Integer count = counts.iterator().next();
					count++;
					jdbcTemplate.update("update phrase_count set phrase_count= ? where phrase = ? ", count,
							phraseCount.getPhrase());
				} else {
					jdbcTemplate.update("insert into phrase_count (phrase, phrase_count) values (?, ?)",
							phraseCount.getPhrase(), 1);
				}
			}
		}
	}
}

class PhraseCount {
	private final String phrase;
	private final int count;

	public PhraseCount(String word, int count) {
		this.phrase = word;
		this.count = count;
	}

	public String getPhrase() {
		return phrase;
	}

	public int getCount() {
		return count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((phrase == null) ? 0 : phrase.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PhraseCount other = (PhraseCount) obj;
		if (phrase == null) {
			if (other.phrase != null)
				return false;
		} else if (!phrase.equals(other.phrase))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
