package Main.model; 
import java.util.ArrayList;

public record ReadResult(ArrayList<Double> values) {

    public ReadResult(ArrayList<Double> values) {
        this.values = values;
    }
    
}
