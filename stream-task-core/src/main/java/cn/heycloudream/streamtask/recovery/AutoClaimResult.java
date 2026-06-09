package cn.heycloudream.streamtask.recovery;

import java.util.List;

public record AutoClaimResult(
        String nextStartId,
        List<ClaimedStreamRecord> records
) {
    public static AutoClaimResult empty() {
        return new AutoClaimResult("0-0", List.of());
    }
}
