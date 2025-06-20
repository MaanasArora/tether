import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
} from '@xyflow/react';

import React, { useState } from 'react';

import {
  forceSimulation,
  forceManyBody,
  forceCenter,
  forceCollide,
  forceLink,
} from 'd3-force';

import '@xyflow/react/dist/style.css';

import { nodeTypes } from './nodes';
import { edgeTypes } from './edges';
import { useEffect } from 'react';
import { getDomainRelations } from './api/base';

export default function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const [showEdges, setShowEdges] = useState(false);

  const performSimulation = (nodes: any[], edges: any[]) => {
    const simNodes = nodes.map((n) => ({ ...n, fx: null, fy: null }));
    const simLinks = edges.map((e) => ({
      id: `${e.source}-${e.target}`,
      source: e.source,
      target: e.target,
    }));
    const simulation = forceSimulation(simNodes)
      .force('charge', forceManyBody().strength(-350))
      .force('center', forceCenter(400, 300))
      .force('collide', forceCollide(70))
      .force(
        'link',
        forceLink(simLinks)
          .id((d: any) => d.id)
          .distance(100)
      )
      .stop();
    for (let i = 0; i < 300; ++i) simulation.tick();
    const finalNodes = simulation.nodes().map((node: any) => ({
      ...node,
      position: { x: node.x, y: node.y },
    }));
    const finalEdges = simulation
      .force('link')
      .links()
      .map((link: any) => ({
        id: `${link.source.id}-${link.target.id}`,
        source: link.source.id,
        target: link.target.id,
      }));
    setNodes(finalNodes);
    setEdges(finalEdges);
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await getDomainRelations();
        if (response) {
          const { nodes: fetchedNodes, edges: fetchedEdges } = response;

          const updatedNodes = fetchedNodes.map((node: any) => ({
            ...node,
            id: node.id.toString(),
            position: { x: 0, y: 0 }, // Initialize positions to (0, 0)
            data: {
              label: node.name || node.columns?.[0]?.name || node.id,
              columns: node.columns || [], // Ensure columns is an array
            },
            type: 'table-domain', // Use the custom node type
          }));
          const updatedEdges = fetchedEdges.map((edge: any) => ({
            ...edge,
            id: `${edge.source}-${edge.target}`,
            source: edge.source.toString(),
            target: edge.target.toString(),
          }));

          setNodes(updatedNodes);
          setEdges(updatedEdges);

          const onInitialLoad = () => {
            setTimeout(() => {
              performSimulation(updatedNodes, updatedEdges);
            }, 0); // Delay to ensure nodes and edges are set
          };
          onInitialLoad();
        }
      } catch (error) {
        console.error('Error fetching domain relations:', error);
      }
    };
    fetchData();
  }, [setEdges, setNodes]);

  return (
    <ReactFlow
      nodes={nodes}
      nodeTypes={nodeTypes}
      onNodesChange={onNodesChange}
      edges={showEdges ? edges : []}
      edgeTypes={edgeTypes}
      onEdgesChange={onEdgesChange}
      fitView>
      <Background />
      <MiniMap />
      <Controls />
    </ReactFlow>
  );
}
