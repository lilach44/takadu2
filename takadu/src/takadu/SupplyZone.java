package takadu;

public class SupplyZone {
	private String zoneName;
	private String parentZoneName;
	
	public SupplyZone(String zoneName, String parentZoneName){
		this.zoneName = zoneName;
		this.parentZoneName = parentZoneName;
	}

	public String getZoneName() {
		return zoneName;
	}

	public void setZoneName(String zoneName) {
		this.zoneName = zoneName;
	}

	public String getParentZoneName() {
		return parentZoneName;
	}

	public void setParentZoneName(String parentZoneName) {
		this.parentZoneName = parentZoneName;
	}
	
	
}
