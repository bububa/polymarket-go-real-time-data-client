package polymarketrealtime

import (
	"fmt"
	"sync"
	"time"
)

// Subscription represents a subscription to a topic and message type
type Subscription struct {
	Topic   Topic       `json:"topic"`
	Type    MessageType `json:"type"`
	Filters string      `json:"filters,omitempty"`

	ClobAuth  *ClobAuth  `json:"clob_auth,omitempty"`
	GammaAuth *GammaAuth `json:"gamma_auth,omitempty"`
}

// ClobAuth contains authentication information for CLOB subscriptions
type ClobAuth struct {
	Key        string `json:"key"`
	Secret     string `json:"secret"`
	Passphrase string `json:"passphrase"`
}

// GammaAuth contains authentication information for Gamma subscriptions
type GammaAuth struct {
	Address string `json:"address"`
}

// WsClient interface for backward compatibility
type WsClient interface {
	// Connect establishes a WebSocket connection to the server
	Connect() error

	// Disconnect closes the WebSocket connection
	Disconnect() error

	// Subscribe sends a subscription message to the server
	Subscribe(subscriptions []Subscription) error

	// Unsubscribe sends an unsubscription message to the server
	Unsubscribe(subscriptions []Subscription) error
}

// Client is a thin wrapper around baseClient for backward compatibility
// It uses the RealtimeProtocol which supports multiple topics
// Starting from the migration, it also supports dual-connection mode:
// - RTDS connection for non-CLOB topics
// - CLOB WebSocket connection for CLOB topics
type Client struct {
	// RTDS connection for non-CLOB topics
	*baseClient

	// CLOB connections (one for market, one for user)
	clobMu           sync.Mutex
	clobMarketClient *baseClient
	clobUserClient   *baseClient

	*RealtimeTypedSubscriptionHandler

	// Options for creating CLOB clients (without withRouter, which is RTDS-specific)
	clobOpts []ClientOption

	// CLOB host from config (if set)
	clobHost string

	// Router for routing messages (shared between RTDS and CLOB)
	router MessageRouter
}

// New creates a new client using the baseClient infrastructure
// This provides backward compatibility while using the improved baseClient implementation
// It automatically manages dual connections: RTDS for non-CLOB topics, CLOB WebSocket for CLOB topics
func New(opts ...ClientOption) *Client {
	// Create RTDS protocol for non-CLOB topics
	protocol := NewRealtimeProtocol()

	router := NewRealtimeMessageRouter()

	// Extract ClobHost from user options (before adding withRouter)
	// This ensures we get the ClobHost value without side effects
	userConfig := &Config{}
	for _, opt := range opts {
		opt(userConfig)
	}
	clobHost := userConfig.ClobHost

	// Store user options (without withRouter) for creating CLOB clients later
	// CLOB clients will use withRouter separately
	userOpts := append([]ClientOption{}, opts...)

	// Now add withRouter and create baseClient for RTDS
	opts = append(opts, withRouter(router))
	base := newBaseClient(protocol, opts...)
	cli := &Client{
		baseClient: base,
		clobOpts:   userOpts, // Store user options (without withRouter) for CLOB clients
		clobHost:   clobHost,
		router:     router, // Store router for CLOB clients
	}

	handler := NewRealtimeTypedSubscriptionHandler(cli, router)

	cli.RealtimeTypedSubscriptionHandler = handler
	return cli
}

// Connect establishes WebSocket connections to the servers
// It automatically connects to RTDS for non-CLOB topics
// CLOB connections are created on-demand when CLOB subscriptions are made
func (c *Client) Connect() error {
	// Always connect to RTDS (for non-CLOB topics)
	return c.baseClient.connect()
}

// Disconnect closes all WebSocket connections
func (c *Client) Disconnect() error {
	var errs []error

	// Disconnect RTDS connection
	if err := c.baseClient.disconnect(); err != nil {
		errs = append(errs, err)
	}

	// Disconnect CLOB market connection if exists
	if c.clobMarketClient != nil {
		if err := c.clobMarketClient.disconnect(); err != nil {
			errs = append(errs, err)
		}
	}

	// Disconnect CLOB user connection if exists
	if c.clobUserClient != nil {
		if err := c.clobUserClient.disconnect(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during disconnect: %v", errs)
	}

	return nil
}

// Subscribe sends subscription requests to the appropriate server
// CLOB subscriptions are automatically routed to CLOB WebSocket
// Non-CLOB subscriptions are routed to RTDS
func (c *Client) Subscribe(subscriptions []Subscription) error {
	// Split subscriptions into CLOB and non-CLOB
	var rtdsSubs []Subscription
	var clobSubs []Subscription

	for _, sub := range subscriptions {
		if isClobTopic(sub.Topic) {
			clobSubs = append(clobSubs, sub)
		} else {
			rtdsSubs = append(rtdsSubs, sub)
		}
	}

	var errs []error

	// Subscribe to RTDS for non-CLOB topics
	if len(rtdsSubs) > 0 {
		if err := c.baseClient.subscribe(rtdsSubs); err != nil {
			errs = append(errs, fmt.Errorf("RTDS subscription error: %w", err))
		}
	}

	// Subscribe to CLOB WebSocket for CLOB topics
	if len(clobSubs) > 0 {
		if err := c.subscribeToClob(clobSubs); err != nil {
			errs = append(errs, fmt.Errorf("CLOB subscription error: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("subscription errors: %v", errs)
	}

	return nil
}

// Unsubscribe sends unsubscription requests to the appropriate server
func (c *Client) Unsubscribe(subscriptions []Subscription) error {
	// Split subscriptions into CLOB and non-CLOB
	var rtdsSubs []Subscription
	var clobSubs []Subscription

	for _, sub := range subscriptions {
		if isClobTopic(sub.Topic) {
			clobSubs = append(clobSubs, sub)
		} else {
			rtdsSubs = append(rtdsSubs, sub)
		}
	}

	var errs []error

	// Unsubscribe from RTDS for non-CLOB topics
	if len(rtdsSubs) > 0 {
		if err := c.baseClient.unsubscribe(rtdsSubs); err != nil {
			errs = append(errs, fmt.Errorf("RTDS unsubscription error: %w", err))
		}
	}

	// Unsubscribe from CLOB WebSocket for CLOB topics
	if len(clobSubs) > 0 {
		if err := c.unsubscribeFromClob(clobSubs); err != nil {
			errs = append(errs, fmt.Errorf("CLOB unsubscription error: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("unsubscription errors: %v", errs)
	}

	return nil
}

// subscribeToClob handles CLOB subscriptions by routing to appropriate CLOB endpoint
func (c *Client) subscribeToClob(subscriptions []Subscription) error {
	// Group subscriptions by endpoint type
	marketSubs := []Subscription{}
	userSubs := []Subscription{}

	for _, sub := range subscriptions {
		endpointType := getClobEndpointType(sub)
		if endpointType == ClobEndpointMarket {
			marketSubs = append(marketSubs, sub)
		} else {
			userSubs = append(userSubs, sub)
		}
	}

	var errs []error

	// Handle market subscriptions
	if len(marketSubs) > 0 {
		needsConnect := false
		c.clobMu.Lock()
		if c.clobMarketClient == nil {
			// Create CLOB market client on-demand
			protocol := NewClobProtocol(ClobEndpointMarket)
			// Use ClobHost from options if set, otherwise use protocol default
			clobOpts := append([]ClientOption{}, c.clobOpts...)
			if c.getClobHost() != "" {
				clobOpts = append(clobOpts, WithHost(c.getClobHost()))
			}
			// Add router for CLOB messages (same router as RTDS)
			if c.router != nil {
				clobOpts = append(clobOpts, withRouter(c.router))
			}
			c.clobMarketClient = newBaseClient(protocol, clobOpts...)
			needsConnect = true
		}
		// Release the lock BEFORE connect() to avoid re-entry deadlock:
		// connect() fires onConnectCallback synchronously, which may call
		// back into Subscribe() and attempt to re-acquire clobMu → deadlocks
		// if we hold the lock here.
		c.clobMu.Unlock()

		if needsConnect {
			if err := c.clobMarketClient.connect(); err != nil {
				return fmt.Errorf("failed to connect to CLOB market endpoint: %w", err)
			}
			// Give the connection a moment to stabilize before subscribing
			// This helps avoid race conditions where readMessages detects a close before subscribe runs
			time.Sleep(100 * time.Millisecond)

			// Check if connection is still alive after the delay
			c.clobMarketClient.connMu.RLock()
			conn := c.clobMarketClient.conn
			c.clobMarketClient.connMu.RUnlock()

			if conn == nil {
				return fmt.Errorf("CLOB market connection closed immediately after connect")
			}

			c.clobMarketClient.internal.mu.RLock()
			isClosed := c.clobMarketClient.internal.connClosed
			c.clobMarketClient.internal.mu.RUnlock()

			if isClosed {
				return fmt.Errorf("CLOB market connection closed immediately after connect")
			}
		}

		// Re-acquire the lock for the subscribe call
		c.clobMu.Lock()
		if err := c.clobMarketClient.subscribe(marketSubs); err != nil {
			errs = append(errs, fmt.Errorf("failed to subscribe to CLOB market: %w", err))
		}
		c.clobMu.Unlock()
	}

	// Handle user subscriptions
	if len(userSubs) > 0 {
		needsConnect := false
		c.clobMu.Lock()
		if c.clobUserClient == nil {
			// Create CLOB user client on-demand
			protocol := NewClobProtocol(ClobEndpointUser)
			// Use ClobHost from options if set, otherwise use protocol default
			clobOpts := append([]ClientOption{}, c.clobOpts...)
			if c.getClobHost() != "" {
				clobOpts = append(clobOpts, WithHost(c.getClobHost()))
			}
			// Add router for CLOB messages (same router as RTDS)
			if c.router != nil {
				clobOpts = append(clobOpts, withRouter(c.router))
			}
			c.clobUserClient = newBaseClient(protocol, clobOpts...)
			needsConnect = true
		}
		// Same lock-release-before-connect pattern as market subs above
		c.clobMu.Unlock()

		if needsConnect {
			if err := c.clobUserClient.connect(); err != nil {
				return fmt.Errorf("failed to connect to CLOB user endpoint: %w", err)
			}

			// TODO: Handle authentication for user endpoint
			// This may require sending auth message after connection
		}

		// Re-acquire the lock for the subscribe call
		c.clobMu.Lock()
		if err := c.clobUserClient.subscribe(userSubs); err != nil {
			errs = append(errs, fmt.Errorf("failed to subscribe to CLOB user: %w", err))
		}
		c.clobMu.Unlock()
	}

	if len(errs) > 0 {
		return fmt.Errorf("CLOB subscription errors: %v", errs)
	}

	return nil
}

// unsubscribeFromClob handles CLOB unsubscriptions
func (c *Client) unsubscribeFromClob(subscriptions []Subscription) error {
	// Group subscriptions by endpoint type
	marketSubs := []Subscription{}
	userSubs := []Subscription{}

	for _, sub := range subscriptions {
		endpointType := getClobEndpointType(sub)
		if endpointType == ClobEndpointMarket {
			marketSubs = append(marketSubs, sub)
		} else {
			userSubs = append(userSubs, sub)
		}
	}

	c.clobMu.Lock()
	defer c.clobMu.Unlock()

	var errs []error

	// Handle market unsubscriptions
	if len(marketSubs) > 0 && c.clobMarketClient != nil {
		if err := c.clobMarketClient.unsubscribe(marketSubs); err != nil {
			errs = append(errs, fmt.Errorf("failed to unsubscribe from CLOB market: %w", err))
		}
	}

	// Handle user unsubscriptions
	if len(userSubs) > 0 && c.clobUserClient != nil {
		if err := c.clobUserClient.unsubscribe(userSubs); err != nil {
			errs = append(errs, fmt.Errorf("failed to unsubscribe from CLOB user: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("CLOB unsubscription errors: %v", errs)
	}

	return nil
}

// getClobHost returns the CLOB host if set, otherwise returns empty string
func (c *Client) getClobHost() string {
	return c.clobHost
}
