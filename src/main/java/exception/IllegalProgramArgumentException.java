package exception;

public class IllegalProgramArgumentException extends RuntimeException {
    public IllegalProgramArgumentException(String message) {
        super(message);
    }
}
