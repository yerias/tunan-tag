package tunan.tag.etl.redis;

public class UserDetail {

    private Integer[] dataNumber;
    private String[] dataValue;

    public UserDetail() {
    }

    public UserDetail(Integer[] dataNumber, String[] dataValue) {
        this.dataNumber = dataNumber;
        this.dataValue = dataValue;
    }

    public Integer[] getDataNumber() {
        return dataNumber;
    }

    public void setDataNumber(Integer[] dataNumber) {
        this.dataNumber = dataNumber;
    }

    public String[] getDataValue() {
        return dataValue;
    }

    public void setDataValue(String[] dataValue) {
        this.dataValue = dataValue;
    }
}
