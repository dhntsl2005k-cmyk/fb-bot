/**
 * ============================================================================
 * CLOUDFLARE WORKER: FACEBOOK UID BATCH SCANNER (GATEWAY QUÉT EDGE SIÊU TỐC)
 * ============================================================================
 * - Miễn phí 100.000 request/ngày từ Cloudflare
 * - Quét phân tán qua hàng triệu IP Anycast toàn cầu của Cloudflare
 * - Không bao giờ bị Facebook chặn IP hay giới hạn tốc độ (Rate Limit)
 * ============================================================================
 */

export default {
  async fetch(request, env, ctx) {
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    // Xử lý Preflight CORS
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);

    // Health check endpoint
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response(
        JSON.stringify({
          status: "online",
          service: "FB UID Edge Scanner Worker",
          version: "2.0.0",
          time: new Date().toISOString()
        }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // Endpoint quét 1 UID đơn lẻ qua GET: /check?uid=100083928192
    if (url.pathname === "/check" && request.method === "GET") {
      const uid = url.searchParams.get("uid");
      if (!uid) {
        return new Response(JSON.stringify({ error: "Thiếu tham số uid" }), {
          status: 400,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
      }
      const result = await checkSingleUid(uid.trim());
      return new Response(JSON.stringify(result), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // Endpoint quét hàng loạt (Batch Check) qua POST: /check-batch
    // Body: { "uids": ["100083928192", "4", "100076543210"] }
    if (url.pathname === "/check-batch" && request.method === "POST") {
      try {
        const body = await request.json();
        const uids = body.uids;

        if (!Array.isArray(uids) || uids.length === 0) {
          return new Response(
            JSON.stringify({ error: "Danh sách uids không hợp lệ hoặc trống" }),
            { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
          );
        }

        // Giới hạn tối đa 100 UID mỗi request để tránh timeout Cloudflare
        const limitedUids = uids.slice(0, 100);

        // Quét song song tất cả UID bằng hàng triệu IP Edge của Cloudflare
        const checkPromises = limitedUids.map(uid => checkSingleUid(String(uid).trim()));
        const results = await Promise.all(checkPromises);

        return new Response(
          JSON.stringify({
            success: true,
            total: results.length,
            results: results
          }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } }
        );
      } catch (err) {
        return new Response(
          JSON.stringify({ error: "Lỗi xử lý JSON: " + err.message }),
          { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
        );
      }
    }

    return new Response("FB Scanner Worker Ready! Hãy gửi POST đến /check-batch", {
      status: 200,
      headers: corsHeaders
    });
  },
};

/**
 * Hàm kiểm tra 1 UID qua Graph API Picture của Facebook
 */
async function checkSingleUid(uid) {
  if (!uid) {
    return { uid, status: "error", reason: "UID rỗng" };
  }

  const fbUrl = `https://graph.facebook.com/v19.0/${uid}/picture?redirect=0`;

  try {
    const response = await fetch(fbUrl, {
      method: "GET",
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "application/json"
      }
    });

    if (response.ok) {
      const data = await response.json();
      const picUrl = data?.data?.url || "";
      const isSilhouette = !!data?.data?.is_silhouette;
      const hasDimensions = data?.data?.height && data?.data?.width;

      if (picUrl.includes("static.xx.fbcdn.net") && !hasDimensions) {
        return {
          uid,
          status: "dead",
          avatar: picUrl,
          isSilhouette,
          reason: "Ảnh đại diện mặc định tĩnh (UID Die / Không tồn tại)"
        };
      }

      return {
        uid,
        status: "alive",
        avatar: picUrl,
        isSilhouette,
        reason: "Tài khoản đang hoạt động (LIVE)"
      };
    } else {
      let reason = "Tài khoản bị vô hiệu hóa / Checkpoint / Không tồn tại";
      try {
        const errJson = await response.json();
        if (errJson?.error?.message) {
          reason = errJson.error.message;
        }
      } catch (_) {}

      return {
        uid,
        status: "dead",
        avatar: "",
        reason: reason
      };
    }
  } catch (e) {
    return {
      uid,
      status: "error",
      avatar: "",
      reason: e.message || "Lỗi kết nối từ Cloudflare tới Facebook"
    };
  }
}
