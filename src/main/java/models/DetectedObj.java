package mvrpl.tensorflow.objdetect.models;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DetectedObj {
    private String label;
    private float score;

    private Box box = new Box();

    public DetectedObj(){

    }

    public DetectedObj(String label, float score, float[] box) {
        this.label = label;
        this.score = score;
        this.box = new Box(box);
    }

    public String getLabel() {
        return label;
    }

    public float getScore() {
        return score;
    }

    public Box getBox() {
        return box;
    }
}