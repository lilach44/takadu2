package takadu;

import java.util.ArrayList;

/**
 * this class represents a node of a bi-directional zone date tree.
 *
 */
public class ZoneNode {
	private ZoneNode parent;
	private String zoneName;
	private ArrayList<ZoneNode> children;
	
	public ZoneNode(String zoneName){
		this.zoneName = zoneName;
		this.parent = null;
		children = new ArrayList<ZoneNode>();
	}
	
	public String getZoneName() {
		return zoneName;
	}
	public void setZoneName(String zoneName) {
		this.zoneName = zoneName;
	}
	public ArrayList<ZoneNode> getChildren() {
		return children;
	}

	public void addChild(ZoneNode childToAdd)
	{
		children.add(childToAdd);
	}
	public ZoneNode getParent() {
		return parent;
	}

	public void setParent(ZoneNode parent) {
		this.parent = parent;
	}
}
