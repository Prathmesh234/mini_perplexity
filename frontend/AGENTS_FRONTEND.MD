# Frontend Application Documentation

## Overview
The frontend is a web-based user interface for the Mini Perplexity search engine. It provides a clean, intuitive interface for users to submit queries and view search results.

## Current Status
**Not yet implemented.** This document serves as a planning guide for the frontend application.

## Planned Architecture

### Technology Stack (Proposed)
- **Framework**: React with TypeScript (or Next.js for SSR)
- **Styling**: Tailwind CSS or vanilla CSS
- **State Management**: React hooks (useState, useEffect)
- **HTTP Client**: Fetch API or Axios
- **Build Tool**: Vite or Create React App

### Key Features

1. **Search Interface**
   - Clean search bar with submit button
   - Real-time query input
   - Loading states during search
   - Error handling and user feedback

2. **Results Display**
   - Card-based layout for search results
   - Display: chunk text, relevance score, document ID
   - Pagination or infinite scroll (optional)
   - Highlight query terms in results (optional)

3. **User Experience**
   - Responsive design (mobile + desktop)
   - Fast, snappy interactions
   - Clear visual hierarchy
   - Accessibility considerations (ARIA labels, keyboard navigation)

### Component Structure (Planned)

```
src/
├── components/
│   ├── SearchBar.tsx          # Input field and submit button
│   ├── SearchResults.tsx      # Container for results
│   ├── ResultCard.tsx         # Individual result item
│   └── LoadingSpinner.tsx     # Loading indicator
├── hooks/
│   └── useSearch.ts           # Custom hook for API calls
├── services/
│   └── api.ts                 # API client for backend
├── types/
│   └── search.ts              # TypeScript interfaces
├── App.tsx                    # Main app component
└── main.tsx                   # Entry point
```

### API Integration

**Connection Method**: SSH Tunnel (Development)
```bash
ssh -L 8002:localhost:8002 ubuntu@<lambda-labs-ip>
```

**API Endpoint**:
```typescript
const API_URL = 'http://localhost:8002/search-db';

async function search(query: string, k: number = 5) {
  const response = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, k })
  });
  return response.json();
}
```

### Design Mockup (Conceptual)

```
┌─────────────────────────────────────────┐
│  Mini Perplexity                        │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │ Search...                      🔍 │ │
│  └───────────────────────────────────┘ │
│                                         │
│  ┌─────────────────────────────────────┐
│  │ Result 1                            │
│  │ Score: 0.95                         │
│  │ "The dry leaves have a very..."     │
│  └─────────────────────────────────────┘
│                                         │
│  ┌─────────────────────────────────────┐
│  │ Result 2                            │
│  │ Score: 0.87                         │
│  │ "Architecture can be seen as..."    │
│  └─────────────────────────────────────┘
└─────────────────────────────────────────┘
```

### Next Steps
1. Initialize React/Next.js project
2. Set up project structure and routing
3. Create SearchBar component
4. Implement API service layer
5. Build ResultCard component
6. Add loading and error states
7. Style with CSS/Tailwind
8. Test with real queries via SSH tunnel
9. Add responsive design
10. Deploy (Vercel, Netlify, or similar)

### Environment Variables (Planned)
```
VITE_API_URL=http://localhost:8002
```

---

## Notes
- CORS is already enabled on the retriever service
- Consider adding query history (localStorage)
- May want to add filters (by score, document type, etc.)
- Future: Add authentication if needed
