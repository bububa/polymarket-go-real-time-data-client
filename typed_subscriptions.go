package polymarketrealtime

import (
	"encoding/json"
	"fmt"
)

// getMapKeys returns all keys from a map[string]interface{}
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// RealtimeTypedSubscriptionHandler provides type-safe subscription handlers for different message types
//
// This handler combines subscription management with message routing. It internally maintains
// a RealtimeMessageRouter that routes incoming messages to registered callbacks.
type RealtimeTypedSubscriptionHandler struct {
	client WsClient
	router *RealtimeMessageRouter
}

// NewRealtimeTypedSubscriptionHandler creates a new typed subscription handler for real-time data
// The client parameter can be nil and set later using SetClient()
func NewRealtimeTypedSubscriptionHandler(client WsClient, router *RealtimeMessageRouter) *RealtimeTypedSubscriptionHandler {
	return &RealtimeTypedSubscriptionHandler{
		client: client,
		router: router,
	}
}

// GetRouter returns the internal message router
// Use this with WithRouter option to automatically route messages
//
// Example:
//
//	typedSub := NewRealtimeTypedSubscriptionHandler(nil)
//	client := New(
//	    WithRouter(typedSub.GetRouter()),
//	    WithLogger(NewLogger()),
//	)
//	typedSub.SetClient(client)
func (h *RealtimeTypedSubscriptionHandler) GetRouter() *RealtimeMessageRouter {
	return h.router
}

// SetClient sets the client for this handler
// This is useful when you create the handler before the client
func (h *RealtimeTypedSubscriptionHandler) SetClient(client WsClient) {
	h.client = client
}

// Activity subscription handlers

// ActivityTradesCallback is the callback function for activity trades messages
type ActivityTradesCallback func(trade Trade) error

// SubscribeToActivityTrades subscribes to activity trades with a typed callback
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple markets/assets and receive data for all of them.
//
// Filter format: {"event_slug":"string"} OR {"market_slug":"string"}
// filter: Optional ActivityFilter to filter by market or event slug
func (h *RealtimeTypedSubscriptionHandler) SubscribeToActivityTrades(callback ActivityTradesCallback, filter *ActivityFilter) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterActivityTradesHandler(callback)
	}

	filterStr := ""
	if filter != nil {
		var err error
		filterStr, err = filter.ToJSON()
		if err != nil {
			return fmt.Errorf("failed to convert filter to JSON: %w", err)
		}
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicActivity,
			Type:    MessageTypeTrades,
			Filters: filterStr,
		},
	})
}

// ActivityOrdersMatchedCallback is the callback function for activity orders matched messages
type ActivityOrdersMatchedCallback func(trade Trade) error

// SubscribeToActivityOrdersMatched subscribes to activity orders matched with a typed callback
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple markets/assets and receive data for all of them.
//
// Filter format: {"event_slug":"string"} OR {"market_slug":"string"}
// filter: Optional ActivityFilter to filter by market or event slug
func (h *RealtimeTypedSubscriptionHandler) SubscribeToActivityOrdersMatched(filter *ActivityFilter, callback ActivityOrdersMatchedCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterActivityOrdersMatchedHandler(callback)
	}

	filterStr := ""
	if filter != nil {
		var err error
		filterStr, err = filter.ToJSON()
		if err != nil {
			return fmt.Errorf("failed to convert filter to JSON: %w", err)
		}
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicActivity,
			Type:    MessageTypeOrdersMatched,
			Filters: filterStr,
		},
	})
}

// Comments subscription handlers

// CommentCreatedCallback is the callback function for comment created messages
type CommentCreatedCallback func(comment Comment) error

// SubscribeToCommentCreated subscribes to comment created events with a typed callback
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple events/series and receive data for all of them.
//
// Filter format: {"event_slug":"string"} OR {"series_slug":"string"}
// filter: Optional CommentFilter to filter by event or series slug
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCommentCreated(filter *CommentFilter, callback CommentCreatedCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterCommentCreatedHandler(callback)
	}

	filterStr := ""
	if filter != nil {
		var err error
		filterStr, err = filter.ToJSON()
		if err != nil {
			return fmt.Errorf("failed to convert filter to JSON: %w", err)
		}
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicComments,
			Type:    MessageTypeCommentCreated,
			Filters: filterStr,
		},
	})
}

// CommentRemovedCallback is the callback function for comment removed messages
type CommentRemovedCallback func(comment Comment) error

// SubscribeToCommentRemoved subscribes to comment removed events with a typed callback
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple events/series and receive data for all of them.
//
// Filter format: {"event_slug":"string"} OR {"series_slug":"string"}
// filter: Optional CommentFilter to filter by event or series slug
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCommentRemoved(filter *CommentFilter, callback CommentRemovedCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterCommentRemovedHandler(callback)
	}

	filterStr := ""
	if filter != nil {
		var err error
		filterStr, err = filter.ToJSON()
		if err != nil {
			return fmt.Errorf("failed to convert filter to JSON: %w", err)
		}
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicComments,
			Type:    MessageTypeCommentRemoved,
			Filters: filterStr,
		},
	})
}

// ReactionCreatedCallback is the callback function for reaction created messages
type ReactionCreatedCallback func(reaction Reaction) error

// SubscribeToReactionCreated subscribes to reaction created events with a typed callback
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple events/series and receive data for all of them.
//
// Filter format: {"event_slug":"string"} OR {"series_slug":"string"}
// filter: Optional CommentFilter to filter by event or series slug
func (h *RealtimeTypedSubscriptionHandler) SubscribeToReactionCreated(filter *CommentFilter, callback ReactionCreatedCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterReactionCreatedHandler(callback)
	}

	filterStr := ""
	if filter != nil {
		var err error
		filterStr, err = filter.ToJSON()
		if err != nil {
			return fmt.Errorf("failed to convert filter to JSON: %w", err)
		}
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicComments,
			Type:    MessageTypeReactionCreated,
			Filters: filterStr,
		},
	})
}

// ReactionRemovedCallback is the callback function for reaction removed messages
type ReactionRemovedCallback func(reaction Reaction) error

// SubscribeToReactionRemoved subscribes to reaction removed events with a typed callback
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple events/series and receive data for all of them.
//
// Filter format: {"event_slug":"string"} OR {"series_slug":"string"}
// filter: Optional CommentFilter to filter by event or series slug
func (h *RealtimeTypedSubscriptionHandler) SubscribeToReactionRemoved(filter *CommentFilter, callback ReactionRemovedCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterReactionRemovedHandler(callback)
	}

	filterStr := ""
	if filter != nil {
		var err error
		filterStr, err = filter.ToJSON()
		if err != nil {
			return fmt.Errorf("failed to convert filter to JSON: %w", err)
		}
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicComments,
			Type:    MessageTypeReactionRemoved,
			Filters: filterStr,
		},
	})
}

// RFQ subscription handlers

// RFQRequestCallback is the callback function for RFQ request messages
type RFQRequestCallback func(request RFQRequest) error

// SubscribeToRFQRequestCreated subscribes to RFQ request created events
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple RFQ events and receive data for all of them.
//
// Filter format: No filters required for RFQ topic
func (h *RealtimeTypedSubscriptionHandler) SubscribeToRFQRequestCreated(callback RFQRequestCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterRFQRequestHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic: TopicRfq,
			Type:  MessageTypeRequestCreated,
		},
	})
}

// SubscribeToRFQRequestEdited subscribes to RFQ request edited events
//
// This topic SUPPORTS multiple subscriptions. See SubscribeToRFQRequestCreated for details.
func (h *RealtimeTypedSubscriptionHandler) SubscribeToRFQRequestEdited(callback RFQRequestCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterRFQRequestHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic: TopicRfq,
			Type:  MessageTypeRequestEdited,
		},
	})
}

// SubscribeToRFQRequestCanceled subscribes to RFQ request canceled events
//
// This topic SUPPORTS multiple subscriptions. See SubscribeToRFQRequestCreated for details.
func (h *RealtimeTypedSubscriptionHandler) SubscribeToRFQRequestCanceled(callback RFQRequestCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterRFQRequestHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic: TopicRfq,
			Type:  MessageTypeRequestCanceled,
		},
	})
}

// SubscribeToRFQRequestExpired subscribes to RFQ request expired events
//
// This topic SUPPORTS multiple subscriptions. See SubscribeToRFQRequestCreated for details.
func (h *RealtimeTypedSubscriptionHandler) SubscribeToRFQRequestExpired(callback RFQRequestCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterRFQRequestHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic: TopicRfq,
			Type:  MessageTypeRequestExpired,
		},
	})
}

// RFQQuoteCallback is the callback function for RFQ quote messages
type RFQQuoteCallback func(quote RFQQuote) error

// SubscribeToRFQQuoteCreated subscribes to RFQ quote created events
//
// This topic SUPPORTS multiple subscriptions. See SubscribeToRFQRequestCreated for details.
func (h *RealtimeTypedSubscriptionHandler) SubscribeToRFQQuoteCreated(callback RFQQuoteCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterRFQQuoteHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic: TopicRfq,
			Type:  MessageTypeQuoteCreated,
		},
	})
}

// SubscribeToRFQQuoteEdited subscribes to RFQ quote edited events
//
// This topic SUPPORTS multiple subscriptions. See SubscribeToRFQRequestCreated for details.
func (h *RealtimeTypedSubscriptionHandler) SubscribeToRFQQuoteEdited(callback RFQQuoteCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterRFQQuoteHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic: TopicRfq,
			Type:  MessageTypeQuoteEdited,
		},
	})
}

// SubscribeToRFQQuoteCanceled subscribes to RFQ quote canceled events
//
// This topic SUPPORTS multiple subscriptions. See SubscribeToRFQRequestCreated for details.
func (h *RealtimeTypedSubscriptionHandler) SubscribeToRFQQuoteCanceled(callback RFQQuoteCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterRFQQuoteHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic: TopicRfq,
			Type:  MessageTypeQuoteCanceled,
		},
	})
}

// SubscribeToRFQQuoteExpired subscribes to RFQ quote expired events
//
// This topic SUPPORTS multiple subscriptions. See SubscribeToRFQRequestCreated for details.
func (h *RealtimeTypedSubscriptionHandler) SubscribeToRFQQuoteExpired(callback RFQQuoteCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterRFQQuoteHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic: TopicRfq,
			Type:  MessageTypeQuoteExpired,
		},
	})
}

// Crypto Prices subscription handlers

// CryptoPriceCallback is the callback function for crypto price update messages
type CryptoPriceCallback func(price CryptoPrice) error

// SubscribeToCryptoPrices subscribes to crypto price updates
//
// IMPORTANT: The Polymarket WebSocket API for crypto_prices topic only supports
// ONE symbol per connection. Subscribing to a new symbol will REPLACE the previous
// subscription, not add to it. If you need to monitor multiple symbols simultaneously,
// you must create separate client connections for each symbol.
//
// Example of correct usage for multiple symbols:
//
//	// Create separate clients for each symbol
//	solClient := polymarketrealtime.New(...)
//	ethClient := polymarketrealtime.New(...)
//
//	solClient.Connect()
//	ethClient.Connect()
//
//	// Each client subscribes to one symbol
//	solSub := polymarketrealtime.NewRealtimeTypedSubscriptionHandler(solClient)
//	ethSub := polymarketrealtime.NewRealtimeTypedSubscriptionHandler(ethClient)
//
//	solSub.SubscribeToCryptoPrices(nil, polymarketrealtime.NewSOLPriceFilter())
//	ethSub.SubscribeToCryptoPrices(nil, polymarketrealtime.NewETHPriceFilter())
//
// filter: CryptoPriceFilter with symbol (e.g., "btcusdt")
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCryptoPrices(filter *CryptoPriceFilter, callback CryptoPriceCallback) error {
	if filter == nil {
		return fmt.Errorf("filter is required for crypto price subscription")
	}

	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterCryptoPriceHandler(callback)
	}

	filterStr, err := filter.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert filter to JSON: %w", err)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicCryptoPrices,
			Type:    MessageTypeUpdate,
			Filters: filterStr,
		},
	})
}

// SubscribeToCryptoPricesChainlink subscribes to crypto price updates from Chainlink
//
// IMPORTANT: Same limitation as SubscribeToCryptoPrices - only ONE symbol per connection.
// See SubscribeToCryptoPrices documentation for details on handling multiple symbols.
//
// filter: CryptoPriceFilter with symbol (e.g., "btcusdt")
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCryptoPricesChainlink(filter *CryptoPriceFilter, callback CryptoPriceCallback) error {
	if filter == nil {
		return fmt.Errorf("filter is required for crypto price chainlink subscription")
	}

	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterCryptoPriceHandler(callback)
	}

	filterStr, err := filter.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert filter to JSON: %w", err)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicCryptoPricesChainlink,
			Type:    MessageTypeUpdate,
			Filters: filterStr,
		},
	})
}

// Equity Prices subscription handlers

// EquityPriceCallback is the callback function for equity price update messages
type EquityPriceCallback func(price EquityPrice) error

// SubscribeToEquityPrices subscribes to equity price updates
//
// IMPORTANT: Same limitation as SubscribeToCryptoPrices - only ONE symbol per connection.
// See SubscribeToCryptoPrices documentation for details on handling multiple symbols.
//
// filter: EquityPriceFilter with symbol (e.g., "AAPL")
func (h *RealtimeTypedSubscriptionHandler) SubscribeToEquityPrices(filter *EquityPriceFilter, callback EquityPriceCallback) error {
	if filter == nil {
		return fmt.Errorf("filter is required for equity price subscription")
	}

	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterEquityPriceHandler(callback)
	}

	filterStr, err := filter.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert filter to JSON: %w", err)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicEquityPrices,
			Type:    MessageTypeUpdate,
			Filters: filterStr,
		},
	})
}

// CLOB User subscription handlers

// CLOBOrderCallback is the callback function for CLOB order messages
type CLOBOrderCallback func(order CLOBOrder) error

// SubscribeToCLOBUserOrders subscribes to CLOB user orders
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCLOBUserOrders(auth *ClobAuth, callback CLOBOrderCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterCLOBOrderHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:    TopicClobUser,
			Type:     MessageTypeOrder,
			ClobAuth: auth,
		},
	})
}

// CLOBTradeCallback is the callback function for CLOB trade messages
type CLOBTradeCallback func(trade CLOBTrade) error

// SubscribeToCLOBUserTrades subscribes to CLOB user trades
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCLOBUserTrades(auth *ClobAuth, callback CLOBTradeCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterCLOBTradeHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:    TopicClobUser,
			Type:     MessageTypeTrade,
			ClobAuth: auth,
		},
	})
}

// SubscribeToCLOBUserAll subscribes to all CLOB user messages (orders and trades)
//
// Note: This method does not accept a callback parameter. To handle messages, you need to
// register handlers on the router manually:
//
//	typedSub.GetRouter().RegisterCLOBOrderHandler(func(order CLOBOrder) error { ... })
//	typedSub.GetRouter().RegisterCLOBTradeHandler(func(trade CLOBTrade) error { ... })
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCLOBUserAll(auth *ClobAuth) error {
	return h.client.Subscribe([]Subscription{
		{
			Topic:    TopicClobUser,
			Type:     MessageTypeAll,
			ClobAuth: auth,
		},
	})
}

// CLOB Market subscription handlers

// PriceChangesCallback is the callback function for price changes messages
type PriceChangesCallback func(changes PriceChanges) error

// SubscribeToCLOBMarketPriceChanges subscribes to CLOB market price changes
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple token IDs and receive data for all of them.
//
// Filter format: ["100","200",...] (array of token IDs as strings)
// filter: CLOBMarketFilter with token IDs (REQUIRED)
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCLOBMarketPriceChanges(filter *CLOBMarketFilter, callback PriceChangesCallback) error {
	if filter == nil {
		return fmt.Errorf("filter is required for price_change subscription")
	}

	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterPriceChangesHandler(callback)
	}

	filterStr, err := filter.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert filter to JSON: %w", err)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicClobMarket,
			Type:    MessageTypePriceChange,
			Filters: filterStr,
		},
	})
}

// AggOrderbookCallback is the callback function for aggregated orderbook messages
type AggOrderbookCallback func(orderbook AggOrderbook) error

// SubscribeToCLOBMarketAggOrderbook subscribes to CLOB market aggregated orderbook
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple token IDs and receive data for all of them.
//
// Filter format: ["100","200",...] (array of token IDs as strings)
// filter: CLOBMarketFilter with token IDs (REQUIRED)
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCLOBMarketAggOrderbook(filter *CLOBMarketFilter, callback AggOrderbookCallback) error {
	if filter == nil {
		return fmt.Errorf("filter is required for agg_orderbook subscription")
	}

	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterAggOrderbookHandler(callback)
	}

	filterStr, err := filter.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert filter to JSON: %w", err)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicClobMarket,
			Type:    MessageTypeAggOrderbook,
			Filters: filterStr,
		},
	})
}

// LastTradePriceCallback is the callback function for last trade price messages
type LastTradePriceCallback func(price LastTradePrice) error

// SubscribeToCLOBMarketLastTradePrice subscribes to CLOB market last trade price
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple token IDs and receive data for all of them.
//
// Filter format: ["100","200",...] (array of token IDs as strings)
// filter: CLOBMarketFilter with token IDs (REQUIRED)
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCLOBMarketLastTradePrice(filter *CLOBMarketFilter, callback LastTradePriceCallback) error {
	if filter == nil {
		return fmt.Errorf("filter is required for last_trade_price subscription")
	}

	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterLastTradePriceHandler(callback)
	}

	filterStr, err := filter.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert filter to JSON: %w", err)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicClobMarket,
			Type:    MessageTypeLastTradePrice,
			Filters: filterStr,
		},
	})
}

// TickSizeChangeCallback is the callback function for tick size change messages
type TickSizeChangeCallback func(change TickSizeChange) error

// SubscribeToCLOBMarketTickSizeChange subscribes to CLOB market tick size changes
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple token IDs and receive data for all of them.
//
// Filter format: ["100","200",...] (array of token IDs as strings)
// filter: CLOBMarketFilter with token IDs (REQUIRED)
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCLOBMarketTickSizeChange(filter *CLOBMarketFilter, callback TickSizeChangeCallback) error {
	if filter == nil {
		return fmt.Errorf("filter is required for tick_size_change subscription")
	}

	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterTickSizeChangeHandler(callback)
	}

	filterStr, err := filter.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert filter to JSON: %w", err)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicClobMarket,
			Type:    MessageTypeTickSizeChange,
			Filters: filterStr,
		},
	})
}

// BestBidAskCallback is the callback function for best bid messages
type BestBidAskCallback func(change BestBidAsk) error

// SubscribeToCLOBMarketBestBidAsk subscribes to CLOB market best bid/ask
//
// This topic SUPPORTS multiple subscriptions in a single connection.
// You can subscribe to multiple token IDs and receive data for all of them.
//
// Filter format: ["100","200",...] (array of token IDs as strings)
// filter: CLOBMarketFilter with token IDs (REQUIRED)
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCLOBMarketBestBidAsk(filter *CLOBMarketFilter, callback BestBidAskCallback) error {
	if filter == nil {
		return fmt.Errorf("filter is required for best bid/ask subscription")
	}

	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterBestBidAskHandler(callback)
	}

	filterStr, err := filter.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to convert filter to JSON: %w", err)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic:   TopicClobMarket,
			Type:    MessageTypeBestBidAsk,
			Filters: filterStr,
		},
	})
}

// ClobMarketCallback is the callback function for CLOB market messages
type ClobMarketCallback func(market ClobMarket) error

// SubscribeToCLOBMarketCreated subscribes to CLOB market created events
//
// This topic SUPPORTS multiple subscriptions in a single connection.
//
// Filter format: No filters required for market_created
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCLOBMarketCreated(callback ClobMarketCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterClobMarketHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic: TopicClobMarket,
			Type:  MessageTypeMarketCreated,
		},
	})
}

// SubscribeToCLOBMarketResolved subscribes to CLOB market resolved events
//
// This topic SUPPORTS multiple subscriptions in a single connection.
//
// Filter format: No filters required for market_resolved
func (h *RealtimeTypedSubscriptionHandler) SubscribeToCLOBMarketResolved(callback ClobMarketCallback) error {
	// Register callback to internal router if provided
	if callback != nil {
		h.router.RegisterClobMarketHandler(callback)
	}

	return h.client.Subscribe([]Subscription{
		{
			Topic: TopicClobMarket,
			Type:  MessageTypeMarketResolved,
		},
	})
}

// RealtimeMessageRouter routes incoming messages to registered typed callbacks
type RealtimeMessageRouter struct {
	// Activity handlers
	activityTradesHandlers        []ActivityTradesCallback
	activityOrdersMatchedHandlers []ActivityOrdersMatchedCallback

	// Comments handlers
	commentCreatedHandlers  []CommentCreatedCallback
	commentRemovedHandlers  []CommentRemovedCallback
	reactionCreatedHandlers []ReactionCreatedCallback
	reactionRemovedHandlers []ReactionRemovedCallback

	// RFQ handlers
	rfqRequestHandlers []RFQRequestCallback
	rfqQuoteHandlers   []RFQQuoteCallback

	// Price handlers
	cryptoPriceHandlers []CryptoPriceCallback
	equityPriceHandlers []EquityPriceCallback

	// CLOB User handlers
	clobOrderHandlers []CLOBOrderCallback
	clobTradeHandlers []CLOBTradeCallback

	// CLOB Market handlers
	priceChangesHandlers   []PriceChangesCallback
	aggOrderbookHandlers   []AggOrderbookCallback
	lastTradePriceHandlers []LastTradePriceCallback
	tickSizeChangeHandlers []TickSizeChangeCallback
	bestBidAskHandlers     []BestBidAskCallback
	clobMarketHandlers     []ClobMarketCallback
}

// NewRealtimeMessageRouter creates a new typed message router
func NewRealtimeMessageRouter() *RealtimeMessageRouter {
	return &RealtimeMessageRouter{}
}

// RegisterActivityTradesHandler registers a handler for activity trades
func (r *RealtimeMessageRouter) RegisterActivityTradesHandler(handler ActivityTradesCallback) {
	r.activityTradesHandlers = append(r.activityTradesHandlers, handler)
}

// RegisterActivityOrdersMatchedHandler registers a handler for activity orders matched
func (r *RealtimeMessageRouter) RegisterActivityOrdersMatchedHandler(handler ActivityOrdersMatchedCallback) {
	r.activityOrdersMatchedHandlers = append(r.activityOrdersMatchedHandlers, handler)
}

// RegisterCommentCreatedHandler registers a handler for comment created events
func (r *RealtimeMessageRouter) RegisterCommentCreatedHandler(handler CommentCreatedCallback) {
	r.commentCreatedHandlers = append(r.commentCreatedHandlers, handler)
}

// RegisterCommentRemovedHandler registers a handler for comment removed events
func (r *RealtimeMessageRouter) RegisterCommentRemovedHandler(handler CommentRemovedCallback) {
	r.commentRemovedHandlers = append(r.commentRemovedHandlers, handler)
}

// RegisterReactionCreatedHandler registers a handler for reaction created events
func (r *RealtimeMessageRouter) RegisterReactionCreatedHandler(handler ReactionCreatedCallback) {
	r.reactionCreatedHandlers = append(r.reactionCreatedHandlers, handler)
}

// RegisterReactionRemovedHandler registers a handler for reaction removed events
func (r *RealtimeMessageRouter) RegisterReactionRemovedHandler(handler ReactionRemovedCallback) {
	r.reactionRemovedHandlers = append(r.reactionRemovedHandlers, handler)
}

// RegisterRFQRequestHandler registers a handler for RFQ request messages
func (r *RealtimeMessageRouter) RegisterRFQRequestHandler(handler RFQRequestCallback) {
	r.rfqRequestHandlers = append(r.rfqRequestHandlers, handler)
}

// RegisterRFQQuoteHandler registers a handler for RFQ quote messages
func (r *RealtimeMessageRouter) RegisterRFQQuoteHandler(handler RFQQuoteCallback) {
	r.rfqQuoteHandlers = append(r.rfqQuoteHandlers, handler)
}

// RegisterCryptoPriceHandler registers a handler for crypto price updates
func (r *RealtimeMessageRouter) RegisterCryptoPriceHandler(handler CryptoPriceCallback) {
	r.cryptoPriceHandlers = append(r.cryptoPriceHandlers, handler)
}

// RegisterEquityPriceHandler registers a handler for equity price updates
func (r *RealtimeMessageRouter) RegisterEquityPriceHandler(handler EquityPriceCallback) {
	r.equityPriceHandlers = append(r.equityPriceHandlers, handler)
}

// RegisterCLOBOrderHandler registers a handler for CLOB order messages
func (r *RealtimeMessageRouter) RegisterCLOBOrderHandler(handler CLOBOrderCallback) {
	r.clobOrderHandlers = append(r.clobOrderHandlers, handler)
}

// RegisterCLOBTradeHandler registers a handler for CLOB trade messages
func (r *RealtimeMessageRouter) RegisterCLOBTradeHandler(handler CLOBTradeCallback) {
	r.clobTradeHandlers = append(r.clobTradeHandlers, handler)
}

// RegisterPriceChangesHandler registers a handler for price changes
func (r *RealtimeMessageRouter) RegisterPriceChangesHandler(handler PriceChangesCallback) {
	r.priceChangesHandlers = append(r.priceChangesHandlers, handler)
}

// RegisterAggOrderbookHandler registers a handler for aggregated orderbook
func (r *RealtimeMessageRouter) RegisterAggOrderbookHandler(handler AggOrderbookCallback) {
	r.aggOrderbookHandlers = append(r.aggOrderbookHandlers, handler)
}

// RegisterLastTradePriceHandler registers a handler for last trade price
func (r *RealtimeMessageRouter) RegisterLastTradePriceHandler(handler LastTradePriceCallback) {
	r.lastTradePriceHandlers = append(r.lastTradePriceHandlers, handler)
}

// RegisterTickSizeChangeHandler registers a handler for tick size changes
func (r *RealtimeMessageRouter) RegisterTickSizeChangeHandler(handler TickSizeChangeCallback) {
	r.tickSizeChangeHandlers = append(r.tickSizeChangeHandlers, handler)
}

// RegisterBestBidAskHandler registers a handler for best bid/ask
func (r *RealtimeMessageRouter) RegisterBestBidAskHandler(handler BestBidAskCallback) {
	r.bestBidAskHandlers = append(r.bestBidAskHandlers, handler)
}

// RegisterClobMarketHandler registers a handler for CLOB market events
func (r *RealtimeMessageRouter) RegisterClobMarketHandler(handler ClobMarketCallback) {
	r.clobMarketHandlers = append(r.clobMarketHandlers, handler)
}

// RouteMessage routes a raw message to the appropriate typed handlers
func (r *RealtimeMessageRouter) RouteMessage(data []byte) error {
	// Handle empty message
	if len(data) == 0 {
		return fmt.Errorf("empty message")
	}

	// Log message format for debugging
	dataStr := string(data)
	if len(dataStr) > 500 {
		dataStr = dataStr[:500] + "..."
	}
	// firstChar := byte(0)
	// if len(data) > 0 {
	// 	firstChar = data[0]
	// }
	// fmt.Printf("[RouteMessage] Received message (len=%d, first_char=%c): %s\n", len(data), firstChar, dataStr)

	// Check if it's an array format (some CLOB messages come as arrays)
	var clobArray []map[string]interface{}
	if err := json.Unmarshal(data, &clobArray); err == nil && len(clobArray) > 0 {
		// fmt.Printf("[RouteMessage] Detected ARRAY format with %d elements\n", len(clobArray))
		// Process each element in the array
		for i, elem := range clobArray {
			// fmt.Printf("[RouteMessage] Processing array element %d with keys: %v\n", i, getMapKeys(elem))
			if err := r.routeCLOBMessage(elem, data); err != nil {
				// fmt.Printf("[RouteMessage] Failed to route array element %d: %v\n", i, err)
				return fmt.Errorf("failed to route array element %d: %w", i, err)
			}
		}
		return nil
	}

	// Try to parse as standard RTDS message format
	var msg Message
	if err := json.Unmarshal(data, &msg); err == nil && msg.Topic != "" && msg.Type != "" {
		// fmt.Printf("[RouteMessage] Successfully parsed as RTDS format: topic=%s, type=%s\n", msg.Topic, msg.Type)
		return r.routeStandardMessage(msg)
	}

	// fmt.Printf("[RouteMessage] Failed to parse as RTDS format, trying CLOB format\n")

	// Try CLOB format as single object
	var clobMsg map[string]interface{}
	if err := json.Unmarshal(data, &clobMsg); err != nil {
		// fmt.Printf("[RouteMessage] Failed to parse as CLOB map format: %v\n", err)
		// Print raw data with length info for debugging
		maxLen := len(data)
		if maxLen > 200 {
			maxLen = 200
		}
		return fmt.Errorf("failed to unmarshal message: %w (len=%d, data=%q)", err, len(data), string(data[:maxLen]))
	}

	// fmt.Printf("[RouteMessage] Successfully parsed as CLOB map format with keys: %v\n", getMapKeys(clobMsg))
	return r.routeCLOBMessage(clobMsg, data)
}

// routeCLOBMessage routes a single CLOB message object
func (r *RealtimeMessageRouter) routeCLOBMessage(clobMsg map[string]interface{}, originalData []byte) error {
	var msg Message

	// Try to extract topic and type from CLOB message
	// Extract topic
	if topic, ok := clobMsg["topic"].(string); ok {
		msg.Topic = Topic(topic)
	} else if channel, ok := clobMsg["channel"].(string); ok {
		// Map channel to topic
		switch channel {
		case "market":
			msg.Topic = TopicClobMarket
		case "user":
			msg.Topic = TopicClobUser
		}
	} else {
		// If no topic/channel, check event_type to determine channel
		// User channel messages have event_type: "trade" or "order"
		// Market channel messages have event_type: "book", "price_change", "last_trade_price", etc.
		if eventType, ok := clobMsg["event_type"].(string); ok {
			// Check if it's a user channel message
			if eventType == "trade" || eventType == "order" {
				msg.Topic = TopicClobUser
				// Map event_type to MessageType for user channel
				if eventType == "trade" {
					msg.Type = MessageTypeTrade
				} else if eventType == "order" {
					msg.Type = MessageTypeOrder
				}
			} else {
				// Market channel message
				msg.Topic = TopicClobMarket
				// Map event_type to MessageType
				switch eventType {
				case "book":
					msg.Type = MessageTypeAggOrderbook
				case "price_change":
					msg.Type = MessageTypePriceChange
				case "last_trade_price":
					msg.Type = MessageTypeLastTradePrice
				case "tick_size_change":
					msg.Type = MessageTypeTickSizeChange
				case "best_bid_ask":
					msg.Type = MessageTypeBestBidAsk
				case "new_market":
					msg.Type = MessageTypeMarketCreated
				case "market_resolved":
					msg.Type = MessageTypeMarketResolved
				default:
					msg.Type = MessageType(eventType)
				}
			}
		}
	}

	// Extract type (if not already set from event_type)
	if msg.Type == "" {
		if msgType, ok := clobMsg["type"].(string); ok {
			msg.Type = MessageType(msgType)
		} else if eventType, ok := clobMsg["event_type"].(string); ok {
			// Map event_type to MessageType based on channel
			if msg.Topic == TopicClobUser {
				// User channel
				if eventType == "trade" {
					msg.Type = MessageTypeTrade
				} else if eventType == "order" {
					msg.Type = MessageTypeOrder
				} else {
					msg.Type = MessageType(eventType)
				}
			} else {
				// Market channel
				switch eventType {
				case "book":
					msg.Type = MessageTypeAggOrderbook
				case "price_change":
					msg.Type = MessageTypePriceChange
				case "last_trade_price":
					msg.Type = MessageTypeLastTradePrice
				case "tick_size_change":
					msg.Type = MessageTypeTickSizeChange
				case "best_bid_ask":
					msg.Type = MessageTypeBestBidAsk
				case "new_market":
					msg.Type = MessageTypeMarketCreated
				case "market_resolved":
					msg.Type = MessageTypeMarketResolved
				default:
					msg.Type = MessageType(eventType)
				}
			}
		}
	}

	// Extract payload - for CLOB messages, the entire message is the payload
	clobMsgBytes, err := json.Marshal(clobMsg)
	if err == nil {
		msg.Payload = clobMsgBytes
	} else {
		msg.Payload = originalData
	}

	// If topic is still empty, we can't route the message
	if msg.Topic == "" {
		return fmt.Errorf("unknown topic: %s", msg.Topic)
	}

	return r.routeStandardMessage(msg)
}

// routeStandardMessage routes a standard Message to the appropriate handlers
func (r *RealtimeMessageRouter) routeStandardMessage(msg Message) error {
	switch msg.Topic {
	case TopicActivity:
		return r.routeActivityMessage(msg)
	case TopicComments:
		return r.routeCommentsMessage(msg)
	case TopicRfq:
		return r.routeRFQMessage(msg)
	case TopicCryptoPrices, TopicCryptoPricesChainlink:
		return r.routeCryptoPriceMessage(msg)
	case TopicEquityPrices:
		return r.routeEquityPriceMessage(msg)
	case TopicClobUser:
		return r.routeCLOBUserMessage(msg)
	case TopicClobMarket:
		return r.routeCLOBMarketMessage(msg)
	default:
		return fmt.Errorf("unknown topic: %s", msg.Topic)
	}
}

func (r *RealtimeMessageRouter) routeActivityMessage(msg Message) error {
	switch msg.Type {
	case MessageTypeTrades:
		var trade Trade
		if err := json.Unmarshal(msg.Payload, &trade); err != nil {
			return fmt.Errorf("failed to unmarshal trade: %w", err)
		}
		for _, handler := range r.activityTradesHandlers {
			if err := handler(trade); err != nil {
				return err
			}
		}
	case MessageTypeOrdersMatched:
		var trade Trade
		if err := json.Unmarshal(msg.Payload, &trade); err != nil {
			return fmt.Errorf("failed to unmarshal orders matched: %w", err)
		}
		for _, handler := range r.activityOrdersMatchedHandlers {
			if err := handler(trade); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RealtimeMessageRouter) routeCommentsMessage(msg Message) error {
	switch msg.Type {
	case MessageTypeCommentCreated:
		var comment Comment
		if err := json.Unmarshal(msg.Payload, &comment); err != nil {
			return fmt.Errorf("failed to unmarshal comment: %w", err)
		}
		for _, handler := range r.commentCreatedHandlers {
			if err := handler(comment); err != nil {
				return err
			}
		}
	case MessageTypeCommentRemoved:
		var comment Comment
		if err := json.Unmarshal(msg.Payload, &comment); err != nil {
			return fmt.Errorf("failed to unmarshal comment: %w", err)
		}
		for _, handler := range r.commentRemovedHandlers {
			if err := handler(comment); err != nil {
				return err
			}
		}
	case MessageTypeReactionCreated:
		var reaction Reaction
		if err := json.Unmarshal(msg.Payload, &reaction); err != nil {
			return fmt.Errorf("failed to unmarshal reaction: %w", err)
		}
		for _, handler := range r.reactionCreatedHandlers {
			if err := handler(reaction); err != nil {
				return err
			}
		}
	case MessageTypeReactionRemoved:
		var reaction Reaction
		if err := json.Unmarshal(msg.Payload, &reaction); err != nil {
			return fmt.Errorf("failed to unmarshal reaction: %w", err)
		}
		for _, handler := range r.reactionRemovedHandlers {
			if err := handler(reaction); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RealtimeMessageRouter) routeRFQMessage(msg Message) error {
	switch msg.Type {
	case MessageTypeRequestCreated, MessageTypeRequestEdited, MessageTypeRequestCanceled, MessageTypeRequestExpired:
		var request RFQRequest
		if err := json.Unmarshal(msg.Payload, &request); err != nil {
			return fmt.Errorf("failed to unmarshal RFQ request: %w", err)
		}
		for _, handler := range r.rfqRequestHandlers {
			if err := handler(request); err != nil {
				return err
			}
		}
	case MessageTypeQuoteCreated, MessageTypeQuoteEdited, MessageTypeQuoteCanceled, MessageTypeQuoteExpired:
		var quote RFQQuote
		if err := json.Unmarshal(msg.Payload, &quote); err != nil {
			return fmt.Errorf("failed to unmarshal RFQ quote: %w", err)
		}
		for _, handler := range r.rfqQuoteHandlers {
			if err := handler(quote); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RealtimeMessageRouter) routeCryptoPriceMessage(msg Message) error {
	if msg.Type == MessageTypeUpdate {
		var price CryptoPrice
		if err := json.Unmarshal(msg.Payload, &price); err != nil {
			return fmt.Errorf("failed to unmarshal crypto price: %w", err)
		}
		for _, handler := range r.cryptoPriceHandlers {
			if err := handler(price); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RealtimeMessageRouter) routeEquityPriceMessage(msg Message) error {
	if msg.Type == MessageTypeUpdate {
		var price EquityPrice
		if err := json.Unmarshal(msg.Payload, &price); err != nil {
			return fmt.Errorf("failed to unmarshal equity price: %w", err)
		}
		for _, handler := range r.equityPriceHandlers {
			if err := handler(price); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RealtimeMessageRouter) routeCLOBUserMessage(msg Message) error {
	switch msg.Type {
	case MessageTypeOrder:
		var order CLOBOrder
		if err := json.Unmarshal(msg.Payload, &order); err != nil {
			return fmt.Errorf("failed to unmarshal CLOB order: %w", err)
		}
		for _, handler := range r.clobOrderHandlers {
			if err := handler(order); err != nil {
				return err
			}
		}
	case MessageTypeTrade:
		var trade CLOBTrade
		if err := json.Unmarshal(msg.Payload, &trade); err != nil {
			return fmt.Errorf("failed to unmarshal CLOB trade: %w", err)
		}
		for _, handler := range r.clobTradeHandlers {
			if err := handler(trade); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RealtimeMessageRouter) routeCLOBMarketMessage(msg Message) error {
	switch msg.Type {
	case MessageTypePriceChange:
		var changes PriceChanges
		if err := json.Unmarshal(msg.Payload, &changes); err != nil {
			return fmt.Errorf("failed to unmarshal price changes: %w", err)
		}
		for _, handler := range r.priceChangesHandlers {
			if err := handler(changes); err != nil {
				return err
			}
		}
	case MessageTypeAggOrderbook:
		// Try to unmarshal as array first (when subscribing to multiple token IDs)
		var orderbooks []AggOrderbook
		if err := json.Unmarshal(msg.Payload, &orderbooks); err == nil {
			// Successfully unmarshaled as array, call handler for each orderbook
			for _, orderbook := range orderbooks {
				for _, handler := range r.aggOrderbookHandlers {
					if err := handler(orderbook); err != nil {
						return err
					}
				}
			}
		} else {
			// Not an array, try to unmarshal as single object
			var orderbook AggOrderbook
			if err := json.Unmarshal(msg.Payload, &orderbook); err != nil {
				return fmt.Errorf("failed to unmarshal agg orderbook (tried both array and object): %w", err)
			}
			for _, handler := range r.aggOrderbookHandlers {
				if err := handler(orderbook); err != nil {
					return err
				}
			}
		}
	case MessageTypeLastTradePrice:
		var lastPrice LastTradePrice
		if err := json.Unmarshal(msg.Payload, &lastPrice); err != nil {
			return fmt.Errorf("failed to unmarshal last trade price: %w", err)
		}
		for _, handler := range r.lastTradePriceHandlers {
			if err := handler(lastPrice); err != nil {
				return err
			}
		}
	case MessageTypeTickSizeChange:
		var change TickSizeChange
		if err := json.Unmarshal(msg.Payload, &change); err != nil {
			return fmt.Errorf("failed to unmarshal tick size change: %w", err)
		}
		for _, handler := range r.tickSizeChangeHandlers {
			if err := handler(change); err != nil {
				return err
			}
		}
	case MessageTypeMarketCreated, MessageTypeMarketResolved:
		var market ClobMarket
		if err := json.Unmarshal(msg.Payload, &market); err != nil {
			return fmt.Errorf("failed to unmarshal CLOB market: %w", err)
		}
		for _, handler := range r.clobMarketHandlers {
			if err := handler(market); err != nil {
				return err
			}
		}
	}
	return nil
}
