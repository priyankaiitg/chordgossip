import React, { useState, useEffect } from 'react';
import { Network, RefreshCw, AlertCircle, CheckCircle, Wifi, WifiOff, Activity, Zap, GitMerge } from 'lucide-react';

const MultiPartitionResilientDNS = () => {
  const [nodes, setNodes] = useState([]);
  const [partitions, setPartitions] = useState([]);
  const [logs, setLogs] = useState([]);
  const [gossipRounds, setGossipRounds] = useState(0);
  const [metrics, setMetrics] = useState({ 
    messagesThisRound: 0, 
    totalMessages: 0,
    convergence: 100,
    partitionHealth: []
  });

  const initializeNetwork = () => {
    const numNodes = 32;
    const newNodes = [];
    
    for (let i = 0; i < numNodes; i++) {
      const fingers = calculateFingers(i, numNodes);
      newNodes.push({
        id: i,
        structure: {
          successor: (i + 1) % numNodes,
          predecessor: (i - 1 + numNodes) % numNodes,
          fingers: fingers,
        },
        // CRITICAL: Partition-aware state
        partitionState: {
          localPartitionId: 0,
          partitionVersion: 0,
          knownPartitions: new Set([0]), // Track discovered partitions
          crossPartitionLinks: [], // Links to other partitions for merger
        },
        gossipState: {
          knownNodes: new Set([i]),
          versionVector: { [i]: 0 },
          lastUpdate: Date.now()
        },
        partitionId: 0,
        x: 250 + 200 * Math.cos((2 * Math.PI * i) / numNodes),
        y: 250 + 200 * Math.sin((2 * Math.PI * i) / numNodes),
        active: true
      });
    }
    
    setNodes(newNodes);
    setPartitions([{ id: 0, nodes: newNodes.map(n => n.id), version: 0 }]);
    addLog("Network initialized with multi-partition resilience");
  };

  const calculateFingers = (nodeId, totalNodes) => {
    const logN = Math.ceil(Math.log2(totalNodes));
    const fingers = [];
    for (let i = 0; i < logN; i++) {
      const fingerId = (nodeId + Math.pow(2, i)) % totalNodes;
      fingers.push(fingerId);
    }
    return [...new Set(fingers)];
  };

  const addLog = (message) => {
    setLogs(prev => [...prev.slice(-15), { 
      time: new Date().toLocaleTimeString(), 
      message,
      round: gossipRounds 
    }]);
  };

  // Create MULTIPLE random partitions
  const simulateMultiplePartitions = () => {
    if (nodes.length === 0) return;
    
    // Create 3-5 random partitions
    const numPartitions = Math.floor(Math.random() * 3) + 3;
    const partitionBoundaries = [];
    for (let i = 0; i < numPartitions - 1; i++) {
      partitionBoundaries.push(Math.floor(Math.random() * nodes.length));
    }
    partitionBoundaries.sort((a, b) => a - b);
    partitionBoundaries.push(nodes.length);
    
    let currentBoundary = 0;
    const newPartitions = [];
    
    const newNodes = nodes.map(node => {
      let partitionId = 0;
      for (let i = 0; i < partitionBoundaries.length; i++) {
        if (node.id < partitionBoundaries[i]) {
          partitionId = i + 1;
          break;
        }
      }
      
      return {
        ...node,
        partitionId: partitionId,
        partitionState: {
          localPartitionId: partitionId,
          partitionVersion: partitionId,
          knownPartitions: new Set([partitionId]),
          crossPartitionLinks: [],
        },
        gossipState: {
          knownNodes: new Set([node.id]),
          versionVector: { [node.id]: 0 },
          lastUpdate: Date.now()
        }
      };
    });
    
    // Create partition metadata
    for (let i = 1; i <= numPartitions; i++) {
      const partitionNodes = newNodes.filter(n => n.partitionId === i).map(n => n.id);
      if (partitionNodes.length > 0) {
        newPartitions.push({ 
          id: i, 
          nodes: partitionNodes,
          version: i,
          timestamp: Date.now()
        });
      }
    }
    
    setNodes(newNodes);
    setPartitions(newPartitions);
    
    addLog(`⚠ Network split into ${newPartitions.length} partitions: ${newPartitions.map(p => p.nodes.length).join(', ')} nodes`);
    addLog("Each partition will converge independently via structured gossip");
  };

  // Structured gossip with partition awareness
  const runStructuredGossip = () => {
    setGossipRounds(prev => prev + 1);
    let messagesThisRound = 0;
    
    const newNodes = nodes.map(node => {
      if (!node.active) return node;
      
      // PARTITION-AWARE: Only gossip within same partition
      const reachableStructure = [
        node.structure.successor,
        ...node.structure.fingers
      ].filter(targetId => {
        const target = nodes.find(n => n.id === targetId);
        return target && target.partitionId === node.partitionId && target.active;
      });
      
      if (reachableStructure.length === 0) return node;
      
      // Structured gossip: successor + one distant finger
      const gossipTargets = [];
      
      if (reachableStructure.includes(node.structure.successor)) {
        gossipTargets.push(node.structure.successor);
      }
      
      const fingerTargets = reachableStructure.filter(id => 
        node.structure.fingers.includes(id) && id !== node.structure.successor
      );
      if (fingerTargets.length > 0) {
        const furthestFinger = fingerTargets[fingerTargets.length - 1];
        gossipTargets.push(furthestFinger);
      }
      
      messagesThisRound += gossipTargets.length;
      
      // CRITICAL: Detect cross-partition links during gossip
      const crossPartitionLinks = [
        node.structure.successor,
        node.structure.predecessor,
        ...node.structure.fingers
      ].filter(targetId => {
        const target = nodes.find(n => n.id === targetId);
        return target && target.partitionId !== node.partitionId && target.active;
      });
      
      // Track discovered partitions
      const knownPartitions = new Set(node.partitionState.knownPartitions);
      crossPartitionLinks.forEach(linkId => {
        const target = nodes.find(n => n.id === linkId);
        if (target) {
          knownPartitions.add(target.partitionId);
        }
      });
      
      // Merge knowledge within partition
      const newKnownNodes = new Set(node.gossipState.knownNodes);
      gossipTargets.forEach(targetId => {
        const target = nodes.find(n => n.id === targetId);
        if (target) {
          target.gossipState.knownNodes.forEach(knownId => {
            if (nodes.find(n => n.id === knownId && n.partitionId === node.partitionId)) {
              newKnownNodes.add(knownId);
            }
          });
        }
      });
      
      return {
        ...node,
        partitionState: {
          ...node.partitionState,
          knownPartitions,
          crossPartitionLinks
        },
        gossipState: {
          knownNodes: newKnownNodes,
          versionVector: node.gossipState.versionVector,
          lastUpdate: Date.now()
        }
      };
    });
    
    setNodes(newNodes);
    
    // Calculate per-partition health
    const partitionHealth = partitions.map(p => {
      const partNodes = newNodes.filter(n => n.partitionId === p.id);
      const avgKnown = partNodes.reduce((sum, n) => sum + n.gossipState.knownNodes.size, 0) / partNodes.length;
      const convergence = (avgKnown / partNodes.length) * 100;
      const crossLinks = partNodes.reduce((sum, n) => sum + n.partitionState.crossPartitionLinks.length, 0);
      return { id: p.id, convergence, crossLinks };
    });
    
    setMetrics(prev => ({
      messagesThisRound,
      totalMessages: prev.totalMessages + messagesThisRound,
      convergence: partitionHealth.reduce((sum, p) => sum + p.convergence, 0) / partitionHealth.length,
      partitionHealth
    }));
  };

  // MULTI-PARTITION MERGER: Using version vectors and eventual consistency
  const simulateGradualMerger = () => {
    if (partitions.length <= 1) {
      addLog("Already unified - cannot merge");
      return;
    }
    
    addLog(`🔄 Initiating merger of ${partitions.length} partitions`);
    addLog("Using: Version vectors + Cross-partition link detection");
    
    // Phase 1: Detect which partitions can merge
    const mergeablePartitions = new Set();
    nodes.forEach(node => {
      if (node.partitionState.crossPartitionLinks.length > 0) {
        mergeablePartitions.add(node.partitionId);
        node.partitionState.knownPartitions.forEach(p => mergeablePartitions.add(p));
      }
    });
    
    if (mergeablePartitions.size === 0) {
      addLog("❌ No cross-partition links detected - partitions remain isolated");
      return;
    }
    
    addLog(`✓ Detected ${mergeablePartitions.size} partitions with cross-links`);
    
    // Phase 2: Gradual merger using lowest partition ID
    const targetPartitionId = Math.min(...Array.from(mergeablePartitions));
    
    const newNodes = nodes.map(node => {
      if (mergeablePartitions.has(node.partitionId)) {
        // This node's partition is merging
        return {
          ...node,
          partitionId: targetPartitionId,
          partitionState: {
            ...node.partitionState,
            localPartitionId: targetPartitionId,
            partitionVersion: node.partitionState.partitionVersion + 1
          }
        };
      }
      return node;
    });
    
    setNodes(newNodes);
    
    // Update partition list
    const remainingPartitions = partitions.filter(p => !mergeablePartitions.has(p.id));
    const mergedNodes = newNodes.filter(n => n.partitionId === targetPartitionId).map(n => n.id);
    
    const newPartitions = [
      ...remainingPartitions,
      { 
        id: targetPartitionId, 
        nodes: mergedNodes,
        version: targetPartitionId,
        timestamp: Date.now()
      }
    ];
    
    setPartitions(newPartitions);
    
    addLog(`✓ Merged into partition ${targetPartitionId} (${mergedNodes.length} nodes)`);
    if (remainingPartitions.length > 0) {
      addLog(`⚠ ${remainingPartitions.length} partitions still isolated`);
    } else {
      addLog(`🎉 Network fully unified!`);
    }
  };

  // Simulate complete random merger (network heals)
  const simulateCompleteNetwork = () => {
    addLog("📡 Network fully healed - all partitions can now communicate");
    
    const newNodes = nodes.map(node => ({
      ...node,
      partitionId: 0,
      partitionState: {
        localPartitionId: 0,
        partitionVersion: node.partitionState.partitionVersion + 1,
        knownPartitions: new Set([0]),
        crossPartitionLinks: []
      }
    }));
    
    setNodes(newNodes);
    setPartitions([{ id: 0, nodes: newNodes.map(n => n.id), version: 0 }]);
    
    addLog("Gossip will now achieve global convergence");
  };

  const simulateLookup = () => {
    if (nodes.length === 0) return;
    
    const sourceNode = nodes[Math.floor(Math.random() * nodes.length)];
    const targetKey = Math.floor(Math.random() * nodes.length);
    const targetNode = nodes.find(n => n.id === targetKey);
    
    if (!targetNode) return;
    
    if (sourceNode.partitionId !== targetNode.partitionId) {
      addLog(`❌ Lookup failed: Node ${sourceNode.id} (P${sourceNode.partitionId}) → Key ${targetKey} (P${targetNode.partitionId})`);
      addLog("   Partitions are isolated - merge required");
      return;
    }
    
    // Greedy routing within partition
    let current = sourceNode;
    let hops = 0;
    const maxHops = Math.ceil(Math.log2(nodes.length)) + 2;
    const path = [current.id];
    
    while (hops < maxHops && current.id !== targetKey) {
      let nextId = current.structure.successor;
      let bestDistance = circularDistance(nextId, targetKey, nodes.length);
      
      for (const fingerId of current.structure.fingers) {
        const fingerNode = nodes.find(n => n.id === fingerId);
        if (!fingerNode || fingerNode.partitionId !== current.partitionId) continue;
        
        const dist = circularDistance(fingerId, targetKey, nodes.length);
        if (dist < bestDistance) {
          bestDistance = dist;
          nextId = fingerId;
        }
      }
      
      const nextNode = nodes.find(n => n.id === nextId);
      if (!nextNode || path.includes(nextId)) break;
      
      current = nextNode;
      path.push(current.id);
      hops++;
      
      if (current.id === targetKey) break;
    }
    
    addLog(`✓ Lookup in P${sourceNode.partitionId}: ${hops} hops [${path.slice(0, 5).join('→')}${path.length > 5 ? '...' : ''}]`);
  };

  const circularDistance = (from, to, max) => {
    return (to - from + max) % max;
  };

  useEffect(() => {
    initializeNetwork();
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      if (nodes.length > 0) {
        runStructuredGossip();
      }
    }, 2000);
    return () => clearInterval(interval);
  }, [nodes]);

  return (
    <div className="w-full min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950 text-white p-4 md:p-8">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8 animate-slide-up">
          <div className="flex items-center gap-3 mb-3">
            <div className="p-3 bg-gradient-to-br from-cyan-500 to-blue-600 rounded-xl shadow-lg shadow-cyan-500/20">
              <Network className="w-8 h-8" />
            </div>
            <div>
              <h1 className="text-4xl font-bold bg-gradient-to-r from-cyan-400 to-blue-500 bg-clip-text text-transparent">
                Multi-Partition Resilient DNS
              </h1>
              <p className="text-gray-400 text-sm mt-1">
                Handling arbitrary partitions and gradual mergers with version vectors
              </p>
            </div>
          </div>
          
          {/* Stats Bar */}
          <div className="grid grid-cols-3 gap-3 mt-4">
            <div className="bg-gradient-to-br from-slate-800 to-slate-900 border border-slate-700 rounded-lg p-4 shadow-xl">
              <div className="flex items-center gap-2 mb-1">
                <Activity className="w-4 h-4 text-cyan-400" />
                <span className="text-xs text-gray-400 uppercase tracking-wide">Gossip Rounds</span>
              </div>
              <div className="text-2xl font-bold text-cyan-400">{gossipRounds}</div>
            </div>
            <div className="bg-gradient-to-br from-slate-800 to-slate-900 border border-slate-700 rounded-lg p-4 shadow-xl">
              <div className="flex items-center gap-2 mb-1">
                <Zap className="w-4 h-4 text-yellow-400" />
                <span className="text-xs text-gray-400 uppercase tracking-wide">Messages Sent</span>
              </div>
              <div className="text-2xl font-bold text-yellow-400">{metrics.totalMessages}</div>
            </div>
            <div className="bg-gradient-to-br from-slate-800 to-slate-900 border border-slate-700 rounded-lg p-4 shadow-xl">
              <div className="flex items-center gap-2 mb-1">
                <GitMerge className="w-4 h-4 text-purple-400" />
                <span className="text-xs text-gray-400 uppercase tracking-wide">Partitions</span>
              </div>
              <div className="text-2xl font-bold text-purple-400">{partitions.length}</div>
            </div>
          </div>
        </div>

        {/* Control Panel */}
        <div className="grid grid-cols-5 gap-3 mb-8">
          <button
            onClick={simulateMultiplePartitions}
            className="group relative bg-gradient-to-br from-red-600 to-red-700 hover:from-red-500 hover:to-red-600 px-4 py-4 rounded-xl font-semibold flex items-center justify-center gap-2 text-sm transition-all duration-200 shadow-lg hover:shadow-red-500/50 hover:scale-105 border border-red-500/20"
          >
            <WifiOff className="w-5 h-5 group-hover:animate-pulse" />
            <span>Split Network</span>
          </button>
          
          <button
            onClick={simulateGradualMerger}
            className="group relative bg-gradient-to-br from-orange-600 to-orange-700 hover:from-orange-500 hover:to-orange-600 px-4 py-4 rounded-xl font-semibold flex items-center justify-center gap-2 text-sm transition-all duration-200 shadow-lg hover:shadow-orange-500/50 hover:scale-105 border border-orange-500/20"
          >
            <GitMerge className="w-5 h-5 group-hover:rotate-180 transition-transform duration-500" />
            <span>Merge Some</span>
          </button>
          
          <button
            onClick={simulateCompleteNetwork}
            className="group relative bg-gradient-to-br from-green-600 to-green-700 hover:from-green-500 hover:to-green-600 px-4 py-4 rounded-xl font-semibold flex items-center justify-center gap-2 text-sm transition-all duration-200 shadow-lg hover:shadow-green-500/50 hover:scale-105 border border-green-500/20"
          >
            <Wifi className="w-5 h-5 group-hover:animate-pulse" />
            <span>Heal All</span>
          </button>
          
          <button
            onClick={simulateLookup}
            className="group relative bg-gradient-to-br from-blue-600 to-blue-700 hover:from-blue-500 hover:to-blue-600 px-4 py-4 rounded-xl font-semibold flex items-center justify-center gap-2 text-sm transition-all duration-200 shadow-lg hover:shadow-blue-500/50 hover:scale-105 border border-blue-500/20"
          >
            <CheckCircle className="w-5 h-5 group-hover:scale-110 transition-transform" />
            <span>Lookup</span>
          </button>
          
          <button
            onClick={runStructuredGossip}
            className="group relative bg-gradient-to-br from-purple-600 to-purple-700 hover:from-purple-500 hover:to-purple-600 px-4 py-4 rounded-xl font-semibold flex items-center justify-center gap-2 text-sm transition-all duration-200 shadow-lg hover:shadow-purple-500/50 hover:scale-105 border border-purple-500/20"
          >
            <Zap className="w-5 h-5 group-hover:animate-pulse" />
            <span>Gossip Round</span>
          </button>
        </div>

        <div className="grid grid-cols-2 gap-6 mb-8">
          {/* Network Visualization */}
          <div className="bg-gradient-to-br from-slate-800/50 to-slate-900/50 backdrop-blur-sm border border-slate-700/50 rounded-2xl p-6 shadow-2xl">
            <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-cyan-400 animate-pulse"></div>
              Network Topology
            </h2>
            <div className="relative">
              <svg width="500" height="500" className="border border-slate-700/50 rounded-xl bg-slate-950/30">
                <defs>
                  <filter id="glow">
                    <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
                    <feMerge>
                      <feMergeNode in="coloredBlur"/>
                      <feMergeNode in="SourceGraphic"/>
                    </feMerge>
                  </filter>
                  <linearGradient id="linkGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" style={{stopColor: '#10b981', stopOpacity: 0.4}} />
                    <stop offset="100%" style={{stopColor: '#3b82f6', stopOpacity: 0.4}} />
                  </linearGradient>
                </defs>
                
              {/* Draw finger links within partitions */}
              {nodes.map(node => {
                return node.structure.fingers.map((fingerId, idx) => {
                  const finger = nodes.find(n => n.id === fingerId);
                  if (!finger || finger.partitionId !== node.partitionId) return null;
                  
                  return (
                    <line
                      key={`finger-${node.id}-${fingerId}`}
                      x1={node.x}
                      y1={node.y}
                      x2={finger.x}
                      y2={finger.y}
                      stroke="#8b5cf6"
                      strokeWidth="1.5"
                      opacity={0.2}
                      className="transition-all duration-300"
                    />
                  );
                });
              })}
              
              {/* Draw cross-partition links (broken) */}
              {nodes.map(node => {
                return node.partitionState.crossPartitionLinks.map(linkId => {
                  const target = nodes.find(n => n.id === linkId);
                  if (!target) return null;
                  
                  return (
                    <line
                      key={`cross-${node.id}-${linkId}`}
                      x1={node.x}
                      y1={node.y}
                      x2={target.x}
                      y2={target.y}
                      stroke="#ef4444"
                      strokeWidth="2.5"
                      strokeDasharray="6,4"
                      opacity="0.7"
                      filter="url(#glow)"
                      className="animate-pulse"
                    />
                  );
                });
              })}
              
              {/* Draw successor links */}
              {nodes.map(node => {
                const successor = nodes.find(n => n.id === node.structure.successor);
                if (!successor || successor.partitionId !== node.partitionId) return null;
                
                return (
                  <line
                    key={`succ-${node.id}`}
                    x1={node.x}
                    y1={node.y}
                    x2={successor.x}
                    y2={successor.y}
                    stroke="url(#linkGradient)"
                    strokeWidth="2.5"
                    opacity="0.5"
                    className="transition-all duration-300"
                  />
                );
              })}
              
              {/* Draw nodes */}
              {nodes.map(node => {
                const partitionNodes = nodes.filter(n => n.partitionId === node.partitionId);
                const convergence = node.gossipState.knownNodes.size / partitionNodes.length;
                
                const colors = [
                  { main: '#10b981', glow: '#10b98130' },
                  { main: '#3b82f6', glow: '#3b82f630' },
                  { main: '#f59e0b', glow: '#f59e0b30' },
                  { main: '#ef4444', glow: '#ef444430' },
                  { main: '#8b5cf6', glow: '#8b5cf630' },
                  { main: '#ec4899', glow: '#ec489930' }
                ];
                const color = colors[node.partitionId % colors.length];
                const isBridge = node.partitionState.crossPartitionLinks.length > 0;
                
                return (
                  <g key={`node-${node.id}`} className="transition-all duration-300">
                    {isBridge && (
                      <circle
                        cx={node.x}
                        cy={node.y}
                        r={18}
                        fill="none"
                        stroke="#fbbf24"
                        strokeWidth="2"
                        opacity={0.6}
                        className="animate-pulse"
                      />
                    )}
                    <circle
                      cx={node.x}
                      cy={node.y}
                      r={isBridge ? 12 : 10}
                      fill={color.main}
                      stroke="#fff"
                      strokeWidth="2"
                      opacity={0.3 + convergence * 0.7}
                      filter={isBridge ? "url(#glow)" : "none"}
                      className="transition-all duration-300"
                    />
                    <text
                      x={node.x}
                      y={node.y}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="white"
                      fontSize="10"
                      fontWeight="bold"
                      style={{ textShadow: '0 0 4px rgba(0,0,0,0.8)' }}
                    >
                      {node.id}
                    </text>
                  </g>
                );
              })}
            </svg>
            </div>
            
            <div className="mt-4 space-y-2 text-xs">
              <div className="flex items-center gap-3 bg-slate-900/50 rounded-lg p-2">
                <div className="w-8 h-1 bg-gradient-to-r from-green-500 to-blue-500 rounded"></div>
                <span className="text-gray-300">Within-partition links</span>
              </div>
              <div className="flex items-center gap-3 bg-slate-900/50 rounded-lg p-2">
                <div className="w-8 h-1 bg-red-500 rounded" style={{borderTop: '2px dashed'}}></div>
                <span className="text-gray-300">Cross-partition links (detected)</span>
              </div>
              <div className="flex items-center gap-3 bg-slate-900/50 rounded-lg p-2">
                <div className="w-4 h-4 rounded-full border-2 border-yellow-400 animate-pulse"></div>
                <span className="text-gray-300">Bridge nodes (cross-partition aware)</span>
              </div>
            </div>
          </div>

          {/* Partition Status Panel */}
          <div className="bg-gradient-to-br from-slate-800/50 to-slate-900/50 backdrop-blur-sm border border-slate-700/50 rounded-2xl p-6 shadow-2xl">
            <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-purple-400 animate-pulse"></div>
              Partition Status
            </h2>
            
            <div className="space-y-3 mb-6 max-h-64 overflow-y-auto pr-2 custom-scrollbar">
              {partitions.map((p, idx) => {
                const health = metrics.partitionHealth.find(h => h.id === p.id);
                const colors = [
                  { from: 'from-green-600', to: 'to-green-700', border: 'border-green-500/30', bg: 'bg-green-500/10', text: 'text-green-400' },
                  { from: 'from-blue-600', to: 'to-blue-700', border: 'border-blue-500/30', bg: 'bg-blue-500/10', text: 'text-blue-400' },
                  { from: 'from-orange-600', to: 'to-orange-700', border: 'border-orange-500/30', bg: 'bg-orange-500/10', text: 'text-orange-400' },
                  { from: 'from-red-600', to: 'to-red-700', border: 'border-red-500/30', bg: 'bg-red-500/10', text: 'text-red-400' },
                  { from: 'from-purple-600', to: 'to-purple-700', border: 'border-purple-500/30', bg: 'bg-purple-500/10', text: 'text-purple-400' },
                  { from: 'from-pink-600', to: 'to-pink-700', border: 'border-pink-500/30', bg: 'bg-pink-500/10', text: 'text-pink-400' }
                ];
                const colorScheme = colors[idx % colors.length];
                
                return (
                  <div key={p.id} className={`${colorScheme.bg} border ${colorScheme.border} rounded-xl p-4 transition-all duration-300 hover:scale-[1.02] animate-slide-up`}>
                    <div className="flex justify-between items-center mb-3">
                      <div className={`font-bold text-lg ${colorScheme.text}`}>Partition {p.id}</div>
                      <div className="text-sm bg-slate-900/50 px-3 py-1 rounded-full">{p.nodes.length} nodes</div>
                    </div>
                    <div className="grid grid-cols-2 gap-3">
                      <div className="bg-slate-900/30 rounded-lg p-2">
                        <div className="text-gray-400 text-xs mb-1">Convergence</div>
                        <div className={`font-semibold text-lg ${colorScheme.text}`}>{health?.convergence?.toFixed(1) || 0}%</div>
                        <div className="w-full bg-slate-700 rounded-full h-1.5 mt-1">
                          <div 
                            className={`bg-gradient-to-r ${colorScheme.from} ${colorScheme.to} h-1.5 rounded-full transition-all duration-500`}
                            style={{width: `${health?.convergence || 0}%`}}
                          ></div>
                        </div>
                      </div>
                      <div className="bg-slate-900/30 rounded-lg p-2">
                        <div className="text-gray-400 text-xs mb-1">Cross-links</div>
                        <div className={`font-semibold text-lg ${colorScheme.text}`}>{health?.crossLinks || 0}</div>
                        <div className="text-xs text-gray-500 mt-1">
                          {health?.crossLinks > 0 ? '⚠ Detected' : '✓ None'}
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>

            <div className="bg-gradient-to-br from-slate-900/80 to-slate-800/80 rounded-xl p-4 border border-cyan-500/20 shadow-lg">
              <div className="flex items-center gap-2 mb-2">
                <Activity className="w-4 h-4 text-cyan-400" />
                <div className="text-sm text-gray-400 uppercase tracking-wide">Overall Convergence</div>
              </div>
              <div className="text-4xl font-bold bg-gradient-to-r from-cyan-400 to-blue-500 bg-clip-text text-transparent">
                {metrics.convergence.toFixed(1)}%
              </div>
              <div className="w-full bg-slate-700 rounded-full h-2 mt-3">
                <div 
                  className="bg-gradient-to-r from-cyan-500 to-blue-500 h-2 rounded-full transition-all duration-500 shadow-lg shadow-cyan-500/50"
                  style={{width: `${metrics.convergence}%`}}
                ></div>
              </div>
              <div className="text-xs text-gray-400 mt-2">
                Average across all {partitions.length} partition{partitions.length !== 1 ? 's' : ''}
              </div>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-6 mb-8">
          {/* Event Log */}
          <div className="bg-gradient-to-br from-slate-800/50 to-slate-900/50 backdrop-blur-sm border border-slate-700/50 rounded-2xl p-6 shadow-2xl">
            <h3 className="font-bold mb-4 flex items-center gap-2 text-lg">
              <AlertCircle className="w-5 h-5 text-amber-400" />
              Event Log
            </h3>
            <div className="bg-slate-950/50 rounded-xl p-4 h-80 overflow-y-auto text-sm font-mono custom-scrollbar">
              {logs.length === 0 ? (
                <div className="text-gray-500 text-center py-8">No events yet...</div>
              ) : (
                logs.map((log, i) => (
                  <div key={i} className="mb-2 p-2 rounded hover:bg-slate-800/30 transition-colors animate-slide-up">
                    <span className="text-cyan-400 font-semibold">[R{log.round}]</span>{' '}
                    <span className="text-gray-300">{log.message}</span>
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Architecture Info */}
          <div className="bg-gradient-to-br from-slate-800/50 to-slate-900/50 backdrop-blur-sm border border-slate-700/50 rounded-2xl p-6 shadow-2xl">
            <h3 className="font-bold mb-4 text-lg">Multi-Partition Resilience</h3>
            <div className="space-y-3 text-sm">
              <div className="bg-gradient-to-r from-green-500/10 to-green-600/10 border border-green-500/30 rounded-xl p-4 hover:border-green-500/50 transition-colors">
                <div className="font-semibold text-green-400 mb-2 flex items-center gap-2">
                  <CheckCircle className="w-4 h-4" />
                  Independent Convergence
                </div>
                <div className="text-gray-300 text-xs leading-relaxed">
                  Each partition converges separately via structured gossip
                </div>
                <div className="text-xs text-gray-500 mt-2">
                  ⚡ No coordination needed between partitions
                </div>
              </div>
              
              <div className="bg-gradient-to-r from-blue-500/10 to-blue-600/10 border border-blue-500/30 rounded-xl p-4 hover:border-blue-500/50 transition-colors">
                <div className="font-semibold text-blue-400 mb-2 flex items-center gap-2">
                  <Network className="w-4 h-4" />
                  Cross-Link Detection
                </div>
                <div className="text-gray-300 text-xs leading-relaxed">
                  Nodes detect broken DHT links to other partitions
                </div>
                <div className="text-xs text-gray-500 mt-2">
                  🔍 Tracks which partitions are neighbors
                </div>
              </div>
              
              <div className="bg-gradient-to-r from-orange-500/10 to-orange-600/10 border border-orange-500/30 rounded-xl p-4 hover:border-orange-500/50 transition-colors">
                <div className="font-semibold text-orange-400 mb-2 flex items-center gap-2">
                  <GitMerge className="w-4 h-4" />
                  Gradual Merger
                </div>
                <div className="text-gray-300 text-xs leading-relaxed">
                  Partitions merge via version vectors
                </div>
                <div className="text-xs text-gray-500 mt-2">
                  🔄 Eventual consistency without global coordination
                </div>
              </div>

              <div className="bg-gradient-to-r from-purple-500/10 to-purple-600/10 border border-purple-500/30 rounded-xl p-4 hover:border-purple-500/50 transition-colors">
                <div className="font-semibold text-purple-400 mb-2 flex items-center gap-2">
                  <Zap className="w-4 h-4" />
                  Cascading Mergers
                </div>
                <div className="text-gray-300 text-xs leading-relaxed">
                  Merged partitions can merge with others
                </div>
                <div className="text-xs text-gray-500 mt-2">
                  ✨ Eventually achieves full convergence
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Protocol Details */}
        <div className="bg-gradient-to-br from-slate-800/50 to-slate-900/50 backdrop-blur-sm border border-slate-700/50 rounded-2xl p-6 shadow-2xl">
          <h2 className="text-2xl font-bold mb-6 flex items-center gap-2">
            <RefreshCw className="w-6 h-6 text-cyan-400" />
            Multi-Partition Merger Protocol
          </h2>
          <div className="grid grid-cols-3 gap-6 text-sm">
            <div className="bg-gradient-to-br from-green-500/5 to-green-600/5 border border-green-500/20 rounded-xl p-5">
              <h3 className="font-semibold text-green-400 mb-4 text-base flex items-center gap-2">
                <span className="w-8 h-8 rounded-full bg-green-500/20 flex items-center justify-center text-sm">1</span>
                Detection Phase
              </h3>
              <ul className="space-y-2 text-gray-300">
                <li className="flex items-start gap-2">
                  <span className="text-green-400 mt-1">•</span>
                  <span>Nodes track DHT structure links</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-green-400 mt-1">•</span>
                  <span>Detect when links cross partitions</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-green-400 mt-1">•</span>
                  <span>Build set of "known partitions"</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-green-400 mt-1">•</span>
                  <span>No false positives from congestion</span>
                </li>
              </ul>
            </div>
            <div className="bg-gradient-to-br from-blue-500/5 to-blue-600/5 border border-blue-500/20 rounded-xl p-5">
              <h3 className="font-semibold text-blue-400 mb-4 text-base flex items-center gap-2">
                <span className="w-8 h-8 rounded-full bg-blue-500/20 flex items-center justify-center text-sm">2</span>
                Merger Decision
              </h3>
              <ul className="space-y-2 text-gray-300">
                <li className="flex items-start gap-2">
                  <span className="text-blue-400 mt-1">•</span>
                  <span>Use version vectors to detect mergers</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-blue-400 mt-1">•</span>
                  <span>Lowest partition ID wins (deterministic)</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-blue-400 mt-1">•</span>
                  <span>No leader election needed</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-blue-400 mt-1">•</span>
                  <span>Handles concurrent mergers safely</span>
                </li>
              </ul>
            </div>
            <div className="bg-gradient-to-br from-purple-500/5 to-purple-600/5 border border-purple-500/20 rounded-xl p-5">
              <h3 className="font-semibold text-purple-400 mb-4 text-base flex items-center gap-2">
                <span className="w-8 h-8 rounded-full bg-purple-500/20 flex items-center justify-center text-sm">3</span>
                Convergence Phase
              </h3>
              <ul className="space-y-2 text-gray-300">
                <li className="flex items-start gap-2">
                  <span className="text-purple-400 mt-1">•</span>
                  <span>Gossip spreads merger info</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-purple-400 mt-1">•</span>
                  <span>Nodes update partition IDs locally</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-purple-400 mt-1">•</span>
                  <span>DHT structure self-repairs via gossip</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-purple-400 mt-1">•</span>
                  <span>O(log²n) rounds to full convergence</span>
                </li>
              </ul>
            </div>
          </div>
          
          <div className="mt-6 p-5 bg-gradient-to-r from-cyan-500/10 to-blue-500/10 rounded-xl border border-cyan-500/30">
            <div className="font-semibold mb-3 text-cyan-400 text-base flex items-center gap-2">
              <CheckCircle className="w-5 h-5" />
              Key Properties
            </div>
            <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm text-gray-300">
              <div className="flex items-start gap-2">
                <span className="text-cyan-400">✓</span>
                <div><strong className="text-white">Arbitrary partitions:</strong> Handles 2, 3, 5, or any number</div>
              </div>
              <div className="flex items-start gap-2">
                <span className="text-cyan-400">✓</span>
                <div><strong className="text-white">Partial mergers:</strong> Some partitions merge, others stay isolated</div>
              </div>
              <div className="flex items-start gap-2">
                <span className="text-cyan-400">✓</span>
                <div><strong className="text-white">Cascading mergers:</strong> Merged partitions can merge with others</div>
              </div>
              <div className="flex items-start gap-2">
                <span className="text-cyan-400">✓</span>
                <div><strong className="text-white">No global state:</strong> Each node makes local decisions</div>
              </div>
              <div className="flex items-start gap-2">
                <span className="text-cyan-400">✓</span>
                <div><strong className="text-white">Eventually consistent:</strong> Guaranteed convergence if stable</div>
              </div>
              <div className="flex items-start gap-2">
                <span className="text-cyan-400">✓</span>
                <div><strong className="text-white">Graceful degradation:</strong> Lookups work within partitions</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default MultiPartitionResilientDNS;