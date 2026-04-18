package polymarketrealtime

import (
	"encoding/json"
	"fmt"
)

// clobProtocol implements the Protocol interface for the CLOB WebSocket endpoint
// CLOB WebSocket has separate endpoints for user and market data:
// - Market: wss://ws-subscriptions-clob.polymarket.com/ws/market
// - User: wss://ws-subscriptions-clob.polymarket.com/ws/user
type clobProtocol struct {
	endpointType ClobEndpointType // "market" or "user"
}

// ClobEndpointType represents the type of CLOB endpoint
type ClobEndpointType string

const (
	// ClobEndpointMarket is for market data (no auth required)
	ClobEndpointMarket ClobEndpointType = "market"
	// ClobEndpointUser is for user data (auth required)
	ClobEndpointUser ClobEndpointType = "user"
)

// NewClobProtocol creates a new CLOB protocol instance
// endpointType should be "market" or "user"
func NewClobProtocol(endpointType ClobEndpointType) Protocol {
	return &clobProtocol{
		endpointType: endpointType,
	}
}

// GetDefaultHost returns the default WebSocket host for CLOB data
func (p *clobProtocol) GetDefaultHost() string {
	baseURL := "wss://ws-subscriptions-clob.polymarket.com/ws"
	switch p.endpointType {
	case ClobEndpointMarket:
		return baseURL + "/market"
	case ClobEndpointUser:
		return baseURL + "/user"
	default:
		return baseURL + "/market" // Default to market
	}
}

// FormatSubscription formats subscriptions into the CLOB WebSocket protocol message format
// According to official Python example:
// - Market channel: {"assets_ids": [...], "operation": "subscribe"}
// - User channel: {"markets": [...], "type": "user", "auth": {...}}
// Note: Initial connection for market channel uses: {"assets_ids": [...], "type": "market"}
func (p *clobProtocol) FormatSubscription(subscriptions []Subscription) ([]byte, error) {
	if len(subscriptions) == 0 {
		return nil, fmt.Errorf("at least one subscription is required")
	}

	sub := subscriptions[0]

	switch sub.Topic {
	case TopicClobMarket:
		// Market channel uses assets_ids and operation: "subscribe"
		// Parse filters to get token IDs (assets_ids)
		if sub.Filters == "" {
			return nil, fmt.Errorf("filters (assets_ids) are required for market subscriptions")
		}
		var tokenIDs []string
		if err := json.Unmarshal([]byte(sub.Filters), &tokenIDs); err != nil {
			return nil, fmt.Errorf("failed to parse filters as token IDs: %w", err)
		}
		if len(tokenIDs) == 0 {
			return nil, fmt.Errorf("at least one asset_id is required for market subscription")
		}

		msg := map[string]any{
			"assets_ids": tokenIDs,
			"operation":  "subscribe",
		}
		return json.Marshal(msg)

	case TopicClobUser:
		// User channel uses markets, type: "user", and auth
		// According to official Python example: {"markets": [...], "type": "user", "auth": {...}}
		msg := map[string]any{
			"type": "user",
		}
		if sub.Filters != "" {
			var markets []string
			if err := json.Unmarshal([]byte(sub.Filters), &markets); err == nil {
				if len(markets) > 0 {
					msg["markets"] = markets
				}
			}
		}
		// Add auth if provided
		if sub.ClobAuth != nil {
			msg["auth"] = map[string]any{
				"apiKey":     sub.ClobAuth.Key,
				"secret":     sub.ClobAuth.Secret,
				"passphrase": sub.ClobAuth.Passphrase,
			}
		}
		return json.Marshal(msg)

	default:
		return nil, fmt.Errorf("unsupported topic for CLOB protocol: %s", sub.Topic)
	}
}

// ParseMessage parses a raw message into a structured Message format
// CLOB WebSocket messages may have a different format than RTDS
// We need to normalize them to the standard Message format
func (p *clobProtocol) ParseMessage(data []byte) (*Message, error) {
	// First, try to parse as standard RTDS message format
	var msg Message
	if err := json.Unmarshal(data, &msg); err == nil {
		// If it parses successfully and has expected fields, use it
		if msg.Topic != "" && msg.Type != "" {
			return &msg, nil
		}
	}

	// Check if it's an array format (CLOB sometimes sends arrays)
	var clobArray []map[string]interface{}
	if err := json.Unmarshal(data, &clobArray); err == nil && len(clobArray) > 0 {
		// If it's an array, process the first element
		// For arrays with multiple elements, we'll process them one by one in the router
		clobMsg := clobArray[0]
		return p.parseClobMessage(clobMsg, data)
	}

	// If standard format doesn't work, try CLOB-specific format
	// CLOB messages might have different structure
	var clobMsg map[string]interface{}
	if err := json.Unmarshal(data, &clobMsg); err != nil {
		return nil, fmt.Errorf("failed to parse message: %w", err)
	}

	return p.parseClobMessage(clobMsg, data)
}

// parseClobMessage parses a single CLOB message object
func (p *clobProtocol) parseClobMessage(clobMsg map[string]interface{}, originalData []byte) (*Message, error) {
	// Try to extract topic and type from CLOB message
	// This is a best-effort conversion - actual format may vary
	result := &Message{}

	// Extract topic
	if topic, ok := clobMsg["topic"].(string); ok {
		result.Topic = Topic(topic)
	} else if channel, ok := clobMsg["channel"].(string); ok {
		// Map channel to topic
		switch channel {
		case "market":
			result.Topic = TopicClobMarket
		case "user":
			result.Topic = TopicClobUser
		}
	} else {
		// If no topic/channel, but we have event_type, assume it's from market channel
		// (since we know this is a CLOB protocol message)
		if eventType, ok := clobMsg["event_type"].(string); ok {
			result.Topic = TopicClobMarket
			// Map event_type to MessageType
			switch eventType {
			case "book":
				result.Type = MessageTypeAggOrderbook
			case "price_change":
				result.Type = MessageTypePriceChange
			case "last_trade_price":
				result.Type = MessageTypeLastTradePrice
			case "tick_size_change":
				result.Type = MessageTypeTickSizeChange
			case "best_bid_ask":
				result.Type = MessageTypeBestBidAsk
			case "new_market":
				result.Type = MessageTypeMarketCreated
			case "market_resolved":
				result.Type = MessageTypeMarketResolved
			default:
				result.Type = MessageType(eventType)
			}
		}
	}

	// Extract type (if not already set from event_type)
	if result.Type == "" {
		if msgType, ok := clobMsg["type"].(string); ok {
			result.Type = MessageType(msgType)
		} else if eventType, ok := clobMsg["event_type"].(string); ok {
			// Map event_type to MessageType
			switch eventType {
			case "book":
				result.Type = MessageTypeAggOrderbook
			case "price_change":
				result.Type = MessageTypePriceChange
			case "last_trade_price":
				result.Type = MessageTypeLastTradePrice
			case "tick_size_change":
				result.Type = MessageTypeTickSizeChange
			case "best_bid_ask":
				result.Type = MessageTypeBestBidAsk
			case "new_market":
				result.Type = MessageTypeMarketCreated
			case "market_resolved":
				result.Type = MessageTypeMarketResolved
			default:
				result.Type = MessageType(eventType)
			}
		}
	}

	// Extract payload - store the entire message as payload if it's not in standard format
	if result.Topic == "" || result.Type == "" {
		// If we can't determine topic/type, store entire message as payload
		result.Payload = originalData
	} else {
		// Store the data part as payload
		if payload, ok := clobMsg["data"]; ok {
			payloadBytes, err := json.Marshal(payload)
			if err == nil {
				result.Payload = payloadBytes
			} else {
				result.Payload = originalData
			}
		} else {
			// Store the entire clobMsg as payload
			clobMsgBytes, err := json.Marshal(clobMsg)
			if err == nil {
				result.Payload = clobMsgBytes
			} else {
				result.Payload = originalData
			}
		}
	}

	// Extract timestamp if available
	if ts, ok := clobMsg["timestamp"].(float64); ok {
		result.Timestamp = int64(ts)
	} else if ts, ok := clobMsg["timestamp"].(int64); ok {
		result.Timestamp = ts
	}

	return result, nil
}

// GetProtocolName returns a human-readable name for this protocol
func (p *clobProtocol) GetProtocolName() string {
	return fmt.Sprintf("CLOB WebSocket (%s)", string(p.endpointType))
}

// FormatUnsubscribe formats an unsubscribe message
// According to official Python example:
// - Market channel: {"assets_ids": [...], "operation": "unsubscribe"}
// - User channel: (not shown in example, but likely similar format)
func (p *clobProtocol) FormatUnsubscribe(subscriptions []Subscription) ([]byte, error) {
	if len(subscriptions) == 0 {
		return nil, fmt.Errorf("at least one subscription is required")
	}

	sub := subscriptions[0]

	switch sub.Topic {
	case TopicClobMarket:
		// Market channel uses assets_ids and operation: "unsubscribe"
		if sub.Filters == "" {
			return nil, fmt.Errorf("filters (assets_ids) are required for market unsubscriptions")
		}
		var tokenIDs []string
		if err := json.Unmarshal([]byte(sub.Filters), &tokenIDs); err != nil {
			return nil, fmt.Errorf("failed to parse filters as token IDs: %w", err)
		}
		if len(tokenIDs) == 0 {
			return nil, fmt.Errorf("at least one asset_id is required for market unsubscription")
		}

		msg := map[string]interface{}{
			"assets_ids": tokenIDs,
			"operation":  "unsubscribe",
		}
		return json.Marshal(msg)

	case TopicClobUser:
		// User channel unsubscribe (format not shown in example, using similar pattern)
		msg := map[string]interface{}{
			"type": "user",
		}
		if sub.Filters != "" {
			var markets []string
			if err := json.Unmarshal([]byte(sub.Filters), &markets); err == nil {
				if len(markets) > 0 {
					msg["markets"] = markets
				}
			}
		}
		return json.Marshal(msg)

	default:
		return nil, fmt.Errorf("unsupported topic for CLOB protocol: %s", sub.Topic)
	}
}

// Helper function to check if a subscription is for CLOB topic
func isClobTopic(topic Topic) bool {
	return topic == TopicClobUser || topic == TopicClobMarket
}

// Helper function to determine which endpoint a CLOB subscription should use
func getClobEndpointType(sub Subscription) ClobEndpointType {
	if sub.Topic == TopicClobUser {
		return ClobEndpointUser
	}
	return ClobEndpointMarket
}
