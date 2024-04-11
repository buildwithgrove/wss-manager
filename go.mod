module github.com/pokt-foundation/wss-manager

go 1.22.0

// TODO - remove
replace github.com/pokt-foundation/portal-middleware => ../portal-middleware

replace github.com/cosmos/cosmos-sdk => github.com/rollkit/cosmos-sdk v0.47.3-rollkit-v0.10.6-no-fraud-proofs

replace github.com/gogo/protobuf => github.com/regen-network/protobuf v1.3.3-alpha.regen.1

require (
	github.com/google/uuid v1.3.0
	github.com/gorilla/websocket v1.5.1
	github.com/pokt-foundation/portal-http-db/v2 v2.4.1
	github.com/pokt-foundation/portal-middleware v0.0.199
	github.com/pokt-foundation/utils-go v0.11.1
)

require (
	github.com/btcsuite/btcd/btcec/v2 v2.2.0 // indirect
	github.com/btcsuite/btcd/chaincfg/chainhash v1.0.2 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/ethereum/go-ethereum v1.10.26 // indirect
	github.com/joho/godotenv v1.4.0 // indirect
	github.com/pokt-foundation/pocket-go v0.17.0 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	golang.org/x/crypto v0.14.0 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
)
