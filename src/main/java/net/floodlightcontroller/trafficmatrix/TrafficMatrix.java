package net.floodlightcontroller.trafficmatrix;

/**
 * Provides Traffic state in network
 * Used to find best route - taking care of congestion 
 * Other modules can call getBestRoute to find optimal routes in competing routes in a network
 *
 * @author Hasanat Kazmi (hasanatkazmi@gmail.com)
 */

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;


import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFStatisticsRequest;
import org.openflow.protocol.statistics.OFPortStatisticsReply;
import org.openflow.protocol.statistics.OFPortStatisticsRequest;
import org.openflow.protocol.statistics.OFStatistics;
import org.openflow.protocol.statistics.OFStatisticsType;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.internal.OFSwitchImpl;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrafficMatrix extends TimerTask  implements IFloodlightModule, IOFSwitchListener, IFloodlightService, ITrafficMatrixService  {

	public static final int INTERVAL_FOR_QUERYING_NETWORK = 60; //in secs
	public static final long DATA_CANNOT_BE_CALCULATED = -1; // if stats havn't matured: i.e. two reading havn't been recorded and we cant calculate stats which need atleast two heartbeats
	
	protected class Stats{
		// data store
		Map<Long, Switch> switches;
		
		protected class Switch{
			Map<Short, Port> ports;
			
			protected class Port{
				Long theoreticallimit;
				Long received_0=null, received_1=null;
				Long sent_0=null, sent_1=null;
				Date time_0=null, time_1=null;
				
				public Port(Long theoreticallimit ) {
					this.theoreticallimit = theoreticallimit;
				}
								
				public Long getTheoreticalLoad(){
					return this.theoreticallimit;
				}

			    /**
				* This function is called to record new values of bytes received and send on a port of a switch
				* This must be called periodically so that new data about ports can be recorded
				*
				*
				* @param received_bytes 		value of current bytes counter of port which records received bytes
				* @param sent_bytes 			value of current bytes counter of port which records sent bytes 
			    */
				public void newBytes(Long received_bytes, Long sent_bytes){
					received_1=received_0;
					sent_1 = sent_0;
					time_1=time_0;
					received_0=received_bytes;
					sent_0=sent_bytes;
					time_0=new Date();
				}
				
			    /**
				* returns total bytes received + transmitted in last interval (interval is defined as INTERVAL_FOR_QUERYING_NETWORK)
				* 
				*
			    */
				public Long getTrafficInLastSpan(){
					try{
						return received_0-received_1 + sent_0-sent_1;
					}
					catch (Exception e) {
						//will only come here if received_0 or received_1 is null
						return DATA_CANNOT_BE_CALCULATED;
					}
				}
				
			    /**
				* returns speed being utilized on a port.
				* this isn't exact speed being utilized. Its an average of over INTERVAL_FOR_QUERYING_NETWORK time, recorded less than INTERVAL_FOR_QUERYING_NETWORK time ago.
				*
			    */
				public Long getBps(){
					try{
						long deltatime = time_0.getTime() - time_1.getTime(); // in ms
						deltatime = deltatime / 1000; // in secs
						return getTrafficInLastSpan()/deltatime;
					}
					catch (Exception e) { 
						return DATA_CANNOT_BE_CALCULATED;
					}
				}
				
			    /**
				* returns percentage of link capacity being utilized
				* correctness of this percentage is limited by same factors which limit getBps
				*
			    */
				public double getLoadPercentage(){ //returns percentage of load
					try{
						return getBps()/getTheoreticalLoad();
					}
					catch(Exception e){
						//when theoretical load is 0
						return DATA_CANNOT_BE_CALCULATED;
					}
				}
			}
			
			public Switch() {
				// TODO Auto-generated constructor stub
				ports = new HashMap<Short, Port>();
			}
			
			public void addPort(Short portnumber, Long theoraticallimit){
				ports.put(portnumber, new Port(theoraticallimit));
			}

			public void removePort(Short portnumber){
				ports.remove(portnumber);
			}

			public Port getPort(Short portnumber){
				return ports.get(portnumber);
			}
			
			public boolean isPortAdded(Short portnumber){
				return ports.containsKey(portnumber);
			}
		}
		
		public Stats() {
			// TODO Auto-generated constructor stub
			switches = new HashMap<Long, Switch>();
		}
		
		public void addSwitch(Long sw){
			switches.put(sw, new Switch());
		}

		public void removeSwitch(Long sw){
			switches.remove(sw);
		}
		
		public Switch getSwitch(Long sw){
			return switches.get(sw);
		}
		
	}


	protected IFloodlightProviderService floodlightProvider;
	protected static Logger logger;
	protected ILinkDiscoveryService linkDiscovery;
	protected Stats statstore;
	
	//vars for state maintenance
	protected boolean first_sw_discovered = false;
	protected boolean hasreturned = false; //used for stopping a deadlock
	
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
        Collection<Class<? extends IFloodlightService>> l = 
                new ArrayList<Class<? extends IFloodlightService>>();
        l.add(ITrafficMatrixService.class);
        return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
        Map<Class<? extends IFloodlightService>,
        IFloodlightService> m = 
        new HashMap<Class<? extends IFloodlightService>,
        IFloodlightService>();
        // We are the class that implements the service
        m.put(ITrafficMatrixService.class, this);
        return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		// TODO Auto-generated method stub
		Collection<Class<? extends IFloodlightService>> l =
		        new ArrayList<Class<? extends IFloodlightService>>();
	        l.add(IFloodlightProviderService.class);
		    return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		logger = LoggerFactory.getLogger(TrafficMatrix.class);
		linkDiscovery = context.getServiceImpl(ILinkDiscoveryService.class);
		statstore = new Stats();
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		// TODO Auto-generated method stub
		floodlightProvider.addOFSwitchListener(this);	
	}

	@Override
	//run our querying systems as soon as first switch is added to the network
	public void addedSwitch(IOFSwitch sw) {
		// TODO Auto-generated method stub
		
		statstore.addSwitch(sw.getId());
		
		if (!first_sw_discovered){
			first_sw_discovered=true;
			new Timer().schedule(this, 0, INTERVAL_FOR_QUERYING_NETWORK * 1000);//if function doesn't return by this interval, its next periodic execution will be stopped in run() by has_returned var (i.e. its safe you do it like this)
		}   
	}

	@Override
	public void removedSwitch(IOFSwitch sw) {
		// TODO Auto-generated method stub
		statstore.removeSwitch(sw.getId());
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

    /**
	* this is the main function which is called after each interval.
	* this function finds theoretical limits on each port of every switch after querying stats on that switch. It only queries a switch once to find this stat
	* this function also finds current traffic being passed from every port of every switch after querying the switch for stats. These stats are then pushed into stat datastore.
    */
	public void portloadscanner(){
				
		for(IOFSwitch sw: floodlightProvider.getSwitches().values()){
			OFSwitchImpl ofi = (OFSwitchImpl)sw;
			Future<List<OFStatistics>> future;
	        List<OFStatistics> values = null;
	        OFStatisticsRequest req = new OFStatisticsRequest();
	        req.setStatisticType(OFStatisticsType.PORT);
	        int requestLength = req.getLengthU();
	        //
	        OFPortStatisticsRequest specificReq = new OFPortStatisticsRequest();
            specificReq.setPortNumber((short)OFPort.OFPP_NONE.getValue());
            req.setStatistics(Collections.singletonList((OFStatistics)specificReq));
            requestLength += specificReq.getLength();
            //
            req.setLengthU(requestLength);
            try {
                future = ofi.getStatistics(req);
                values = future.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Failure retrieving statistics from switch {}", ofi, e);
            }
            
            for (OFStatistics temp : values){
            	OFPortStatisticsReply reply = (OFPortStatisticsReply) temp;     
            	
            	if(!statstore.getSwitch(sw.getId()).isPortAdded(reply.getPortNumber())) { //ensure that this happens only once for each swith
	            	int currentfeatures = ((OFSwitchImpl)sw).getPort(reply.getPortNumber()).getCurrentFeatures();
	            	long port_brandwidth = currentfeatures;
	            	if (currentfeatures > (1<<6))	port_brandwidth = currentfeatures % (1<<7); //higher bits give very important about copper/optical etc TODO: add this info in capacity calulations
	            	if (port_brandwidth == (1 << 0) || port_brandwidth == (1 << 1))port_brandwidth = 10*1024;//10Mb 
	            	else if (port_brandwidth == (1 << 2) || port_brandwidth == (1 << 3))port_brandwidth = 100*1024;//100Mb 
	            	else if (port_brandwidth == (1 << 4) || port_brandwidth == (1 << 5))port_brandwidth = 1024*1024;//1Gb 
	            	else if (port_brandwidth == (1 << 6))port_brandwidth = 1024*10*1024;//10Gb
	            		            	
	            	statstore.getSwitch(sw.getId()).addPort(reply.getPortNumber(), port_brandwidth);
            	}
            	statstore.getSwitch(sw.getId()).getPort(reply.getPortNumber()).newBytes(reply.getReceiveBytes(), reply.getTransmitBytes());
            	
            	
            	//priting current stats.
            	logger.debug("Sw:" + sw.getId() + 
            			"\tPort: "+ reply.getPortNumber() + 
            			"\t\tTrafficLastSpan: " + statstore.getSwitch(sw.getId()).getPort(reply.getPortNumber()).getTrafficInLastSpan() +
            			"\t\tTheoreticalLoad: " + statstore.getSwitch(sw.getId()).getPort(reply.getPortNumber()).getTheoreticalLoad() +
            			"\t\tPortSpeed: " + statstore.getSwitch(sw.getId()).getPort(reply.getPortNumber()).getBps() + 
            			"\t\tPortLoad%: " + statstore.getSwitch(sw.getId()).getPort(reply.getPortNumber()).getLoadPercentage() 
            			);
            	
            }   
		}
		
	}
	
	/**
	* this function is called by Timer every interval to re-run portloadscanner. 
	* A deadlock mechanism ensures that previous interval's portloadscanner has finished execution. It not, then portloadscanner is not run (ensuring that only one scanner is running at one time)
	* This can be called to update current state immediately 
    */
	public void run() {
		if (hasreturned){
			hasreturned = false;
			logger.debug("Calculating traffix load matrix");
			portloadscanner();			
		}
		hasreturned = true;
	}
	
	/**
	* returns total bandwidth which is available on the link.
	*
    */
	public Long getFreeLinkBrandwidth(Link l){
		long bps = (long)statstore.getSwitch(l.getSrc()).getPort(l.getSrcPort()).getBps();
		long brandwidth = statstore.getSwitch(l.getSrc()).getPort(l.getSrcPort()).getTheoreticalLoad().longValue();
		return new Long(brandwidth - bps);
	}

	
	@Override
	/**
	* a function which can be used by other modules to find optimal route
	* this function hasn't been implemented yet.
	* As a proof of concept, this function is return longest route .
	*
    */
	public Route getBestRoute(LinkedList<Route> routes) {
		// TODO Auto-generated method stub
		
		//As a proof of concept, return largest path (with max hops)
		Route toreturn = null;
		int temp = 0;
		for (Route i : routes){
			if (temp < i.getPath().size() ){
				temp = i.getPath().size();
				toreturn = i;
			}
		}
		
		logger.debug("Longest route: " + toreturn);
		
		return toreturn;
	}
	
}