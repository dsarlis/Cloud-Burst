package org.cloudburst.util;

public class Q4Object implements Comparable {
    private String date;
    private String count;
    private String userList;
    private String text;

    public Q4Object(String date, String count, String userList, String text) {
        this.date = date;
        this.count = count;
        this.userList = userList;
        this.text = text;
    }

    public String getDate() {
        return date;
    }

    public String getCount() {
        return count;
    }

    public String getUserList() {
        return userList;
    }

    public String getText() {
        return text;
    }

    @Override
    public int compareTo(Object o) {
        if (this.count.compareTo(((Q4Object) o).getCount()) == 0) {
            return this.date.compareTo(((Q4Object) o).getDate());
        }
        return -1 * this.count.compareTo(((Q4Object) o).getCount());
    }
}

