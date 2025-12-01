import React, { useEffect, useState } from 'react'
import { SigmaContainer, useLoadGraph } from '@react-sigma/core'
import { useWorkerLayoutForceAtlas2 } from '@react-sigma/layout-forceatlas2'
import Graph from 'graphology'
import "@react-sigma/core/lib/react-sigma.min.css"

interface GraphComponentProps {
    seedPaperId: string
}

const GraphEvents: React.FC<{ onNodeClick: (nodeId: string) => void }> = ({ onNodeClick }) => {
    const loadGraph = useLoadGraph();
    const { start, kill } = useWorkerLayoutForceAtlas2({ settings: { slowDown: 10 } });

    useEffect(() => {
        start();
        return () => kill();
    }, [start, kill]);

    return null;
}

const GraphVisualizer: React.FC<GraphComponentProps> = ({ seedPaperId }) => {
    const [graph, setGraph] = useState<Graph | null>(null);

    useEffect(() => {
        const g = new Graph();
        setGraph(g);

        // Initial fetch
        fetchGraph(seedPaperId, g);
    }, [seedPaperId]);

    const fetchGraph = async (paperId: string, g: Graph) => {
        try {
            const res = await fetch(`/api/graph/expand?paper_id=${paperId}`);
            const data = await res.json();

            data.nodes.forEach((node: any) => {
                if (!g.hasNode(node.id)) {
                    g.addNode(node.id, {
                        label: node.title,
                        size: 10,
                        color: "#FA4F40",
                        x: Math.random(),
                        y: Math.random()
                    });
                }
            });

            data.edges.forEach((edge: any) => {
                if (!g.hasEdge(edge.source, edge.target)) {
                    g.addEdge(edge.source, edge.target, {
                        color: edge.type === 'similarity' ? '#00FF00' : '#CCCCCC',
                        size: edge.type === 'similarity' ? 2 : 1
                    });
                }
            });
        } catch (err) {
            console.error("Failed to fetch graph", err);
        }
    };

    if (!graph) return null;

    return (
        <SigmaContainer style={{ height: "100%", width: "100%" }} graph={graph}>
            <GraphEvents onNodeClick={(id) => fetchGraph(id, graph)} />
        </SigmaContainer>
    )
}

export default GraphVisualizer
