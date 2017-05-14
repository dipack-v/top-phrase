package com.example;

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;

public class FlatFileRecordsWriter implements ItemWriter<List<PhraseCount>> {
	private final FlatFileItemWriter<PhraseCount> flatFileItemWriter;

	public FlatFileRecordsWriter(final FlatFileItemWriter<PhraseCount> flatFileItemWriter) {
		this.flatFileItemWriter = flatFileItemWriter;
	}

	@Override
	public void write(List<? extends List<PhraseCount>> items) throws Exception {
		try {
			flatFileItemWriter.open(new ExecutionContext());
			for (List<PhraseCount> wordCounts : items) {
				flatFileItemWriter.write(wordCounts);
			}
		}

		finally {
			flatFileItemWriter.close();
		}
	}

}
