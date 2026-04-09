package polymarketrealtime

import (
	"encoding/json"
	"time"

	"github.com/shopspring/decimal"
)

// NOTE: This file is for the payloads that are sent by the server.
// More info can be found here: https://github.com/Polymarket/real-time-data-client

// Trade represents a user's trade on a conditional token market.
type Trade struct {
	Asset           string          `json:"asset"`           // ERC1155 token ID of conditional token being traded
	Bio             string          `json:"bio"`             // Bio of the user of the trade
	ConditionID     string          `json:"conditionId"`     // ID of market which is also the CTF condition ID
	EventSlug       string          `json:"eventSlug"`       // Slug of the event
	Icon            string          `json:"icon"`            // URL to the market icon image
	Name            string          `json:"name"`            // Name of the user of the trade
	Outcome         string          `json:"outcome"`         // Human readable outcome of the market
	OutcomeIndex    int             `json:"outcomeIndex"`    // Index of the outcome
	Price           decimal.Decimal `json:"price"`           // Price of the trade
	ProfileImage    string          `json:"profileImage"`    // URL to the user profile image
	ProxyWallet     string          `json:"proxyWallet"`     // Address of the user proxy wallet
	Pseudonym       string          `json:"pseudonym"`       // Pseudonym of the user
	Side            Side            `json:"side"`            // Side of the trade (BUY/SELL)
	Size            decimal.Decimal `json:"size"`            // Size of the trade
	Status          TradeStatus     `json:"status"`          // Status of the match: e.g., MINED
	Slug            string          `json:"slug"`            // Slug of the market
	Timestamp       int64           `json:"timestamp"`       // Timestamp of the trade (milliseconds)
	Time            time.Time       `json:"-"`               // Parsed time from timestamp
	Title           string          `json:"title"`           // Title of the event
	TransactionHash string          `json:"transactionHash"` // Hash of the transaction
}

// UnmarshalJSON implements custom JSON unmarshaling for Trade
func (t *Trade) UnmarshalJSON(data []byte) error {
	type Alias Trade
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(t),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Parse timestamp to time.Time (milliseconds)
	if t.Timestamp != 0 {
		t.Time = time.UnixMilli(t.Timestamp)
	}

	return nil
}

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type TradeStatus string

const (
	TradeStatusMatched                = "MATCHED"
	TradeStatusMined                  = "MINED"
	TradeStatusConfirmed              = "CONFIRMED"
	TradeStatusCanceledMarketResolved = "CANCELED_MARKET_RESOLVED"
)

// Comment represents a comment on an event or series.
type Comment struct {
	ID               string `json:"id"`               // Unique identifier of comment
	Body             string `json:"body"`             // Content of the comment
	ParentEntityType string `json:"parentEntityType"` // Type of the parent entity (Event or Series)
	ParentEntityID   int64  `json:"parentEntityID"`   // ID of the parent entity (changed from float64 to int64)
	ParentCommentID  string `json:"parentCommentID"`  // ID of the parent comment
	UserAddress      string `json:"userAddress"`      // Address of the user
	ReplyAddress     string `json:"replyAddress"`     // Address of the reply user
	CreatedAt        string `json:"createdAt"`        // Creation timestamp
	UpdatedAt        string `json:"updatedAt"`        // Last update timestamp
}

// Reaction represents a reaction to a comment.
type Reaction struct {
	ID           string `json:"id"`           // Unique identifier of reaction
	CommentID    int64  `json:"commentID"`    // ID of the comment (changed from float64 to int64)
	ReactionType string `json:"reactionType"` // Type of the reaction
	Icon         string `json:"icon"`         // Icon representing the reaction
	UserAddress  string `json:"userAddress"`  // Address of the user
	CreatedAt    string `json:"createdAt"`    // Creation timestamp
}

// RFQRequest represents a request to trade a conditional token.
type RFQRequest struct {
	RequestID    string          `json:"requestId"`    // Unique identifier for the request
	ProxyAddress string          `json:"proxyAddress"` // User proxy address
	Market       string          `json:"market"`       // ID of market which is also the CTF condition ID
	Token        string          `json:"token"`        // ERC1155 token ID of conditional token being traded
	Complement   string          `json:"complement"`   // Complement ERC1155 token ID of conditional token being traded
	State        string          `json:"state"`        // Current state of the request
	Side         Side            `json:"side"`         // Indicates buy or sell side
	SizeIn       decimal.Decimal `json:"sizeIn"`       // Input size of the request
	SizeOut      decimal.Decimal `json:"sizeOut"`      // Output size of the request
	Price        decimal.Decimal `json:"price"`        // Price from in/out sizes
	Expiry       int64           `json:"expiry"`       // Expiry timestamp (UNIX format, seconds)
	ExpiryTime   time.Time       `json:"-"`            // Parsed expiry time
}

// UnmarshalJSON implements custom JSON unmarshaling for RFQRequest
func (r *RFQRequest) UnmarshalJSON(data []byte) error {
	type Alias RFQRequest
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Parse expiry to time.Time (seconds)
	if r.Expiry != 0 {
		r.ExpiryTime = time.Unix(r.Expiry, 0)
	}

	return nil
}

// RFQQuote represents a response to a trade request.
type RFQQuote struct {
	QuoteID      string          `json:"quoteId"`      // Unique identifier for the quote
	RequestID    string          `json:"requestId"`    // Associated request identifier
	ProxyAddress string          `json:"proxyAddress"` // User proxy address
	Token        string          `json:"token"`        // ERC1155 token ID of conditional token being traded
	State        string          `json:"state"`        // Current state of the quote
	Side         Side            `json:"side"`         // Indicates buy or sell side
	SizeIn       decimal.Decimal `json:"sizeIn"`       // Input size of the quote
	SizeOut      decimal.Decimal `json:"sizeOut"`      // Output size of the quote
	Condition    string          `json:"condition"`    // ID of market which is also the CTF condition ID
	Complement   string          `json:"complement"`   // Complement ERC1155 token ID of conditional token being traded
	Expiry       int64           `json:"expiry"`       // Expiry timestamp (UNIX format, seconds)
	ExpiryTime   time.Time       `json:"-"`            // Parsed expiry time
}

// UnmarshalJSON implements custom JSON unmarshaling for RFQQuote
func (q *RFQQuote) UnmarshalJSON(data []byte) error {
	type Alias RFQQuote
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(q),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Parse expiry to time.Time (seconds)
	if q.Expiry != 0 {
		q.ExpiryTime = time.Unix(q.Expiry, 0)
	}

	return nil
}

// CryptoPrice represents a cryptocurrency price update.
type CryptoPrice struct {
	Symbol    string          `json:"symbol"`    // Symbol of the asset
	Timestamp int64           `json:"timestamp"` // Timestamp in milliseconds for the update
	Time      time.Time       `json:"-"`         // Parsed time from timestamp
	Value     decimal.Decimal `json:"value"`     // Value at the time of update
}

// UnmarshalJSON implements custom JSON unmarshaling for CryptoPrice
func (c *CryptoPrice) UnmarshalJSON(data []byte) error {
	type Alias CryptoPrice
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(c),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Parse timestamp to time.Time (milliseconds)
	if c.Timestamp != 0 {
		c.Time = time.UnixMilli(c.Timestamp)
	}

	return nil
}

// EquityPrice represents an equity price update.
type EquityPrice struct {
	Symbol    string          `json:"symbol"`    // Symbol of the asset
	Timestamp int64           `json:"timestamp"` // Timestamp in milliseconds for the update
	Time      time.Time       `json:"-"`         // Parsed time from timestamp
	Value     decimal.Decimal `json:"value"`     // Value at the time of update
}

// UnmarshalJSON implements custom JSON unmarshaling for EquityPrice
func (e *EquityPrice) UnmarshalJSON(data []byte) error {
	type Alias EquityPrice
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(e),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Parse timestamp to time.Time (milliseconds)
	if e.Timestamp != 0 {
		e.Time = time.UnixMilli(e.Timestamp)
	}

	return nil
}

// CLOBOrder represents a CLOB user order.
type CLOBOrder struct {
	AssetID         string          `json:"asset_id"`                   // Order's ERC1155 token ID of conditional token
	CreatedAt       string          `json:"created_at"`                 // Order's creation UNIX timestamp
	Expiration      string          `json:"expiration"`                 // Order's expiration UNIX timestamp
	ID              string          `json:"id"`                         // Unique order hash identifier
	MakerAddress    string          `json:"maker_address"`              // Maker's address (funder)
	Market          string          `json:"market"`                     // Condition ID or market identifier
	OrderType       string          `json:"order_type"`                 // Type of order: GTC, GTD, FOK, FAK
	OrderOwner      string          `json:"order_owner,omitempty"`      //
	OriginalSize    decimal.Decimal `json:"original_size"`              // Original size of the order at placement
	Outcome         string          `json:"outcome"`                    // Order outcome: YES / NO
	Owner           string          `json:"owner"`                      // UUID of the order owner
	Price           decimal.Decimal `json:"price"`                      // Order price (e.g., in decimals like 0.5)
	Side            Side            `json:"side"`                       // Side of the trade: BUY or SELL
	SizeMatched     decimal.Decimal `json:"size_matched"`               // Amount of order that has been matched
	Status          string          `json:"status"`                     // Status of the order (e.g., MATCHED)
	Type            string          `json:"type"`                       // Type of update: PLACEMENT, CANCELLATION, FILL, etc.
	AssociateTrades []string        `json:"associate_trades,omitempty"` // Trade IDs this order has been matched in
	Timestamp       string          `json:"timestamp"`                  // Event timestamp in milliseconds
}

// CLOBTrade represents a CLOB user trade.
type CLOBTrade struct {
	AssetID         string           `json:"asset_id"`      // ERC1155 token ID of the conditional token involved in the trade
	FeeRateBps      decimal.Decimal  `json:"fee_rate_bps"`  // Fee rate in basis points (bps)
	ID              string           `json:"id"`            // Unique identifier for the match record
	LastUpdate      string           `json:"last_update"`   // Last update timestamp (UNIX)
	MakerAddress    string           `json:"maker_address"` // Maker's address
	MakerOrders     []CLOBMakerOrder `json:"maker_orders"`  // List of maker orders
	Market          string           `json:"market"`        // Condition ID or market identifier
	MatchTime       string           `json:"matchtime"`     // Match execution timestamp (UNIX)
	Outcome         string           `json:"outcome"`       // Outcome of the market: YES / NO
	Owner           string           `json:"owner"`         // UUID of the taker (owner of the matched order)
	TradeOwner      string           `json:"trade_owner,omitempty"`
	Price           decimal.Decimal  `json:"price"`            // Matched price (in decimal format, e.g., 0.5)
	Side            Side             `json:"side"`             // Taker side of the trade: BUY or SELL
	Size            decimal.Decimal  `json:"size"`             // Total matched size
	Status          TradeStatus      `json:"status"`           // Status of the match: e.g., MINED
	TakerOrderID    string           `json:"taker_order_id"`   // ID of the taker's order
	TransactionHash string           `json:"transaction_hash"` // Transaction hash where the match was settled
	BucketIndex     int              `json:"bucket_index,omitempty"`
	TraderSide      string           `json:"trader_side,omitempty"` // Whether the receiving user was TAKER or MAKER, Available options: TAKER, MAKER
	Timestamp       string           `json:"timestamp"`             // Event timestamp in milliseconds
}

// CLOBMakerOrder represents a maker order in a CLOB trade.
type CLOBMakerOrder struct {
	AssetID       string          `json:"asset_id"`       // ERC1155 token ID of the conditional token of the maker's order
	FeeRateBps    decimal.Decimal `json:"fee_rate_bps"`   // Maker's fee rate in basis points
	MakerAddress  string          `json:"maker_address"`  // Maker's address
	MatchedAmount decimal.Decimal `json:"matched_amount"` // Amount matched from the maker's order
	OrderID       string          `json:"order_id"`       // ID of the maker's order
	Outcome       string          `json:"outcome"`        // Outcome targeted by the maker's order (YES / NO)
	Owner         string          `json:"owner"`          // UUID of the maker
	Price         decimal.Decimal `json:"price"`          // Order price
	Side          Side            `json:"side"`           // Side of the maker: BUY or SELL
}

// PriceChanges represents CLOB market price changes.
type PriceChanges struct {
	Market      string        `json:"m"`  // Condition ID
	PriceChange []PriceChange `json:"pc"` // Price changes by book
	Timestamp   string        `json:"t"`  // Timestamp in milliseconds since epoch (UNIX time * 1000)
}

// PriceChange represents a single price change in CLOB market.
type PriceChange struct {
	AssetID string          `json:"a"`  // Asset identifier
	Hash    string          `json:"h"`  // Unique hash ID of the book snapshot
	Price   decimal.Decimal `json:"p"`  // Price quoted (e.g., 0.5)
	Side    Side            `json:"si"` // Side of the quote: BUY or SELL
	Size    decimal.Decimal `json:"s"`  // Size or volume available at the quoted price (e.g., 0, 100)
	BestAsk decimal.Decimal `json:"ba"` // Best ask price
	BestBid decimal.Decimal `json:"bb"` // Best bid price
}

// AggOrderbook represents CLOB market aggregated orderbook.
type AggOrderbook struct {
	AssetID      string           `json:"asset_id"`       // Asset Id identifier
	Asks         []OrderbookLevel `json:"asks"`           // List of ask aggregated orders (sell side)
	Bids         []OrderbookLevel `json:"bids"`           // List of aggregated bid orders (buy side)
	Hash         string           `json:"hash"`           // Unique hash ID for this orderbook snapshot
	Market       string           `json:"market"`         // Market or condition ID
	MinOrderSize string           `json:"min_order_size"` // Minimum allowed order size
	NegRisk      bool             `json:"neg_risk"`       // NegRisk or not
	TickSize     string           `json:"tick_size"`      // Minimum tick size
	Timestamp    string           `json:"timestamp"`      // Timestamp in milliseconds since epoch (UNIX time * 1000)
}

// OrderbookLevel represents a price level in the orderbook.
type OrderbookLevel struct {
	Price decimal.Decimal `json:"price"` // Price level
	Size  decimal.Decimal `json:"size"`  // Size at that price
}

// LastTradePrice represents CLOB market last trade price.
type LastTradePrice struct {
	AssetID    string          `json:"asset_id"`     // Asset Id identifier
	FeeRateBps decimal.Decimal `json:"fee_rate_bps"` // Fee rate in basis points (bps)
	Market     string          `json:"market"`       // Market or condition ID
	Price      decimal.Decimal `json:"price"`        // Trade price (e.g., 0.5)
	Side       Side            `json:"side"`         // Side of the order: BUY or SELL
	Size       decimal.Decimal `json:"size"`         // Size of the trade
}

// TickSizeChange represents CLOB market tick size changes.
type TickSizeChange struct {
	Market      string          `json:"market"`        // Market or condition ID
	AssetID     []string        `json:"asset_id"`      // Array of two ERC1155 asset ID
	OldTickSize decimal.Decimal `json:"old_tick_size"` // Previous tick size before the change
	NewTickSize decimal.Decimal `json:"new_tick_size"` // Updated tick size after the change
}

// ClobMarket represents CLOB market information.
// This is used for both market_created and market_resolved message types.
type ClobMarket struct {
	Market       string          `json:"market"`         // Market or condition ID
	AssetIDs     []string        `json:"asset_ids"`      // Array of two ERC1155 asset ID identifiers associated with market
	MinOrderSize decimal.Decimal `json:"min_order_size"` // Minimum size allowed for an order
	TickSize     decimal.Decimal `json:"tick_size"`      // Minimum allowable price increment
	NegRisk      bool            `json:"neg_risk"`       // Indicates if the market is negative risk
}
