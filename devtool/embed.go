package devtool

import "embed"

// webDist holds the built web UI (npm run build in web/).
//
//go:embed all:web/dist
var webDist embed.FS
