/*
* Copyright (c) 2001-2011 ssparrow, Inc. All Rights Reserved.
*
* This software is the proprietary information of ssparrow, Inc.
* Use is subject to license terms.
*/
package com.ssparrow.storm.pattern.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei (eaglelion8038@hotmail.com)
 *
 */
public class TransactionItemsetBuildBolt extends BaseRichBolt {
	private OutputCollector collector;
	private Map<String, Set<String>> transactionItemMap;
	
	/**
	 * @param stormConf
	 * @param context
	 * @param collector
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		transactionItemMap=new HashMap<String, Set<String>>();
	}

	/**
	 * @param input
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String tid = input.getString(0);
		Set<String> inItemSet = (Set<String>) input.getValue(1);
		
		Set<String> currentItemSet = transactionItemMap.get(tid);
		
		if(currentItemSet==null){
			currentItemSet=new TreeSet<String>(inItemSet);
			transactionItemMap.put(tid, currentItemSet);
		}else if((inItemSet.size()==1 && !currentItemSet.contains(inItemSet.iterator().next()))||
					(inItemSet.size()>1 && inItemSet.size() != currentItemSet.size())){
			for(String item: currentItemSet){
				if(!inItemSet.contains(item)){
					Set<String> newItemSet=new TreeSet<String>(inItemSet);
					newItemSet.add(item);
					
					System.out.println("*************Built new set"+newItemSet+" from"+inItemSet+"]");
					collector.emit(new Values(newItemSet, tid));
				}
			}
			
			if(inItemSet.size()==1){
				currentItemSet.addAll(inItemSet);
			}
		}
		
		collector.ack(input);
	}

	/**
	 * @param declarer
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("itemset", "tid"));
	}

}
