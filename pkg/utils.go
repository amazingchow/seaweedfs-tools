package pkg

import (
	"encoding/hex"

	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

func FormatFileId(key types.NeedleId, cookie types.Cookie) string {
	bytes := make([]byte, types.NeedleIdSize+types.CookieSize)
	types.NeedleIdToBytes(bytes[0:types.NeedleIdSize], key)
	types.CookieToBytes(bytes[types.NeedleIdSize:types.NeedleIdSize+types.CookieSize], cookie)
	nonzero_index := 0
	for ; bytes[nonzero_index] == 0; nonzero_index++ {
	}
	return hex.EncodeToString(bytes[nonzero_index:])
}
