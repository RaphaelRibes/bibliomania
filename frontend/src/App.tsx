import React, { useState } from 'react'
import Graph from './components/Graph'
import './App.css'

interface Paper {
    id: str
    title: str
    year?: number
    first_author?: str
    venue?: str
    cited_by_count?: number
}

function App() {
    const [query, setQuery] = useState('')
    const [searchResults, setSearchResults] = useState<Paper[]>([])
    const [selectedPaper, setSelectedPaper] = useState<Paper | null>(null)

    const handleSearch = async (e: React.FormEvent) => {
        e.preventDefault()
        if (query.length < 3) return

        try {
            const res = await fetch(`/api/search?q=${encodeURIComponent(query)}`)
            const data = await res.json()
            setSearchResults(data)
        } catch (err) {
            console.error("Search failed", err)
        }
    }

    const handleSelectPaper = (paper: Paper) => {
        setSelectedPaper(paper)
        setSearchResults([]) // Clear search to show graph
    }

    return (
        <div className="app-container">
            <div className="sidebar">
                <h1>Prometheus Local</h1>
                <form onSubmit={handleSearch}>
                    <input
                        type="text"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        placeholder="Search papers..."
                    />
                    <button type="submit">Search</button>
                </form>

                <div className="results">
                    {searchResults.map(p => (
                        <div key={p.id} className="result-item" onClick={() => handleSelectPaper(p)}>
                            <h3>{p.title}</h3>
                            <p>{p.first_author} ({p.year})</p>
                        </div>
                    ))}
                </div>

                {selectedPaper && (
                    <div className="selected-info">
                        <h2>Selected: {selectedPaper.title}</h2>
                        <p>{selectedPaper.first_author}, {selectedPaper.year}</p>
                    </div>
                )}
            </div>

            <div className="graph-area">
                {selectedPaper ? (
                    <Graph seedPaperId={selectedPaper.id} />
                ) : (
                    <div className="placeholder">Select a paper to start exploring</div>
                )}
            </div>
        </div>
    )
}

export default App
