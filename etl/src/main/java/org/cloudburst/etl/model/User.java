package org.cloudburst.etl.model;

import com.google.gson.annotations.SerializedName;

/**
 * GSON Mapping for User.
 */
public class User {

	@SerializedName("id")
	private long userId;

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

}
