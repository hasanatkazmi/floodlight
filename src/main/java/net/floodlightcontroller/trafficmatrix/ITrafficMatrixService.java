package net.floodlightcontroller.trafficmatrix;

import java.util.LinkedList;

import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.routing.Route;

public interface ITrafficMatrixService extends IFloodlightService {

	public Route getBestRoute(LinkedList<Route> routes); 

}
