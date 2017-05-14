package com.example;

import com.google.gson.Gson;

public class PhraseCount {
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
