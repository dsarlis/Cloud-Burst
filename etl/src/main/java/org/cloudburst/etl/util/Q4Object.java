package org.cloudburst.etl.util;

public class Q4Object implements Comparable {
    private String createdAt;
    private String text;

    public Q4Object(String createdAt, String text) {
        this.createdAt = createdAt;
        this.text = text;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public int compareTo(Object o) {
        if (this.createdAt.compareTo(((Q4Object) o).getCreatedAt()) == 0) {
            return this.text.compareTo(((Q4Object) o).getText());
        }
        return this.createdAt.compareTo(((Q4Object) o).getCreatedAt());
    }
}
