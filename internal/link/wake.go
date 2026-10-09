package link

// wake tells client's connection, if there is one, to push what is queued.
func (h *Hub) wake(client string) {
	h.mu.Lock()
	s := h.sessions[client]
	h.mu.Unlock()
	if s != nil {
		s.poke()
	}
}

// WakeAll tells every open connection to push what is queued for its
// client. It is for messages queued in the store directly, not by Send,
// such as those the inbound side routes in a transaction. It never blocks.
func (h *Hub) WakeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, s := range h.sessions {
		s.poke()
	}
}

// poke wakes the session's writer. It never blocks.
func (s *session) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
