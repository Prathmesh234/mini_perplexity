import { useState, useRef, useEffect } from 'react'
import './App.css'

interface Message {
  role: 'user' | 'assistant'
  content: string
  results?: Record<string, string>
}

function App() {
  const [messages, setMessages] = useState<Message[]>([])
  const [inputValue, setInputValue] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
      textareaRef.current.style.height = textareaRef.current.scrollHeight + 'px'
    }
  }, [inputValue])

  const handleSend = async () => {
    if (!inputValue.trim() || isLoading) return

    const userMessage: Message = {
      role: 'user',
      content: inputValue.trim()
    }

    setMessages(prev => [...prev, userMessage])
    setInputValue('')
    setIsLoading(true)

    try {
      // Call the /search endpoint
      const response = await fetch('/search', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query: userMessage.content }),
      })

      if (!response.ok) {
        throw new Error('Search request failed')
      }

      const data = await response.json()
      
      // Extract results (all fields except final_answer)
      const { final_answer, ...results } = data
      
      const assistantMessage: Message = {
        role: 'assistant',
        content: final_answer || 'No answer received',
        results: Object.keys(results).length > 0 ? results : undefined
      }

      setMessages(prev => [...prev, assistantMessage])
    } catch (error) {
      console.error('Error calling /search:', error)
      const errorMessage: Message = {
        role: 'assistant',
        content: 'Sorry, I encountered an error processing your request.'
      }
      setMessages(prev => [...prev, errorMessage])
    } finally {
      setIsLoading(false)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  const handleNewConversation = () => {
    setMessages([])
    setInputValue('')
  }

  return (
    <div className="app-container">
      <header className="app-header">
        <button className="new-conversation-btn" onClick={handleNewConversation}>
          New Conversation
        </button>
        <h1 className="app-title">mini perplexity</h1>
      </header>

      <div className="messages-container">
        {messages.map((message, index) => (
          <div key={index} className={`message ${message.role}`}>
            {message.role === 'user' ? (
              <div className="message-content">{message.content}</div>
            ) : (
              <div className="assistant-message">
                {message.results && Object.keys(message.results).length > 0 && (
                  <ResultsDropdown results={message.results} />
                )}
                <div className="message-content">{message.content}</div>
              </div>
            )}
          </div>
        ))}
        {isLoading && (
          <div className="message assistant">
            <div className="message-content loading">Thinking...</div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      <div className="input-container">
        <textarea
          ref={textareaRef}
          className="message-input"
          placeholder="Ask anything"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={isLoading}
          rows={1}
        />
        <button 
          className="send-btn" 
          onClick={handleSend}
          disabled={!inputValue.trim() || isLoading}
        >
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z" />
          </svg>
        </button>
      </div>
    </div>
  )
}

function ResultsDropdown({ results }: { results: Record<string, string> }) {
  const [isExpanded, setIsExpanded] = useState(false)

  const truncate = (text: string, maxLength: number = 150) => {
    if (text.length <= maxLength) return text
    return text.slice(0, maxLength) + '...'
  }

  return (
    <div className="results-dropdown">
      <button 
        className="results-toggle"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <svg 
          className={`toggle-icon ${isExpanded ? 'expanded' : ''}`}
          width="16" 
          height="16" 
          viewBox="0 0 24 24" 
          fill="none" 
          stroke="currentColor" 
          strokeWidth="2"
        >
          <path d="M9 18l6-6-6-6" />
        </svg>
        <span>Sources ({Object.keys(results).length})</span>
      </button>
      
      {isExpanded && (
        <div className="results-content">
          {Object.entries(results).map(([key, value], index) => (
            <div key={index} className="result-item">
              <span className="result-label">{key}:</span>
              <span className="result-text">{truncate(value)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default App
