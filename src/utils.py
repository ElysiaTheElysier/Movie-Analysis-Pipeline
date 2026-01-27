# src/utils.py
import numpy as np
import difflib


def get_power_score(name, role_dict, global_stats):
    if not name or len(name.strip()) < 2: 
        return np.log1p(global_stats['avg_revenue']), 0, None
    
    if not isinstance(role_dict, dict): 
        return np.log1p(global_stats['avg_revenue']), 0, None
    
    matches = difflib.get_close_matches(name, role_dict.keys(), n=1, cutoff=0.6)
    if matches:
        real_name = matches[0]
        raw_val = role_dict[real_name]['avg_revenue']
        return np.log1p(raw_val), raw_val, real_name
    
    return np.log1p(global_stats['avg_revenue']), 0, None


def analyze_risk_and_safety(budget, raw_pred, dir_raw, cast_raw, is_franchise, overview):
    risk_score = 0
    warnings = []
    
    # Sanity Check: Ngân sách lớn mà người làm vô danh
    is_high_budget = budget > 20_000_000
    is_unknown_crew = (dir_raw < 5) and (cast_raw < 5)
    
    if is_high_budget and is_unknown_crew:
        risk_score += 0.8
        warnings.append("High budget specified but Director and Cast are unknown or have low historical power.")
        return raw_pred * 0.1, raw_pred * 0.05, warnings, risk_score # Trả về đủ 4 giá trị nếu cần

    # Check nội dung
    overview_len = len(str(overview).strip())
    if overview_len < 50:
        risk_score += 0.3
        warnings.append("Overview text is too short to analyze accurately.")
    
    # Check Franchise
    if is_franchise:
        if budget < 10_000_000:
            risk_score += 0.2
            warnings.append("Franchise movie with suspiciously low budget.")
        if cast_raw < 10:
            risk_score += 0.15
            warnings.append("Franchise sequel lacks star power.")
    

    roi = raw_pred / (budget + 1)
    if roi > 8.0 and budget > 5_000_000:
        warnings.append("Predicted ROI is unnaturally high. Adjusting prediction down for realism.")
        raw_pred = raw_pred * 0.6


    safety_margin = 0.15
    if budget > 200_000_000:
        safety_margin = 0.25
        warnings.append("High risk factor for very high budget production.")

    final_pred = raw_pred * (1 - safety_margin - risk_score)
    worst_case = final_pred * 0.5
    
    if final_pred < 0: final_pred = 0
    if worst_case < 0: worst_case = 0

    return final_pred, worst_case, warnings