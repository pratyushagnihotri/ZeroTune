package plangeneratorflink.enumeration;

/** Rulebased enumeration strategy op. */
public class RBESEstimator {

    private String operatorName;
    private int parallelism;
    private double estimatedSelectivity;
    private int estimatedOutputRate;
    private boolean parallelismSet = false;

    public RBESEstimator(String operatorName) {
        setOperatorName(operatorName);
    }

    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
        this.markParallelismSet();
    }

    public double getEstimatedSelectivity() {
        return estimatedSelectivity;
    }

    public void setEstimatedSelectivity(double estimatedSelectivity) {
        this.estimatedSelectivity = estimatedSelectivity;
    }

    public int getEstimatedOutputRate() {
        return estimatedOutputRate;
    }

    public void setEstimatedOutputRate(int estimatedOutputRate) {
        this.estimatedOutputRate = estimatedOutputRate;
    }

    public boolean isParallelismSet() {
        return parallelismSet;
    }

    private void markParallelismSet() {
        this.parallelismSet = true;
    }

    @Override
    public String toString() {
        return "RBESEstimator{" +
                "operatorName='" + operatorName + '\'' +
                ", parallelism=" + parallelism +
                ", estimatedSelectivity=" + estimatedSelectivity +
                ", estimatedOutputRate=" + estimatedOutputRate +
                '}';
    }
}
