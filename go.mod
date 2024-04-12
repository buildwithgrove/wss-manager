module github.com/pokt-foundation/wss-manager

go 1.22.0

replace github.com/cosmos/cosmos-sdk => github.com/rollkit/cosmos-sdk v0.47.3-rollkit-v0.10.6-no-fraud-proofs

replace github.com/gogo/protobuf => github.com/regen-network/protobuf v1.3.3-alpha.regen.1

require (
	github.com/google/uuid v1.3.0
	github.com/gorilla/websocket v1.5.1
	github.com/pokt-foundation/portal-http-db/v2 v2.15.0
	github.com/pokt-foundation/utils-go v0.11.1
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/joho/godotenv v1.4.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	golang.org/x/net v0.17.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
