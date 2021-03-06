package de.bwaldvogel.mongo.exception;

public class MongoServerError extends MongoServerException {

    private static final long serialVersionUID = 4998311355923688257L;
    private int errorCode;
    private String codeName;

    public MongoServerError(int errorCode, String codeName, String message) {
        super(message);
        this.errorCode = errorCode;
        this.codeName = codeName;
    }

    public MongoServerError(int errorCode, String message) {
        this(errorCode, null, message);
    }

    public int getCode() {
        return errorCode;
    }

    public String getCodeName() {
        return codeName;
    }
}
