package takadu;


public class SupplyValue {
	private int supplyValue;
	private boolean isActual;
	
	public SupplyValue(int supplyValue, boolean isActual) {
		this.supplyValue = supplyValue;
		this.isActual = isActual;
	}
	
	public int getSupplyValue() {
		return supplyValue;
	}
	public void setSupplyValue(int supplyValue) {
		this.supplyValue = supplyValue;
	}

	public boolean isActual() {
		return isActual;
	}

	public void setActual(boolean isActual) {
		this.isActual = isActual;
	}

}
