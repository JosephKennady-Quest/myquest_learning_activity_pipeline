/*
Review-only MySQL 8 SQL for one-row-per-user AEL output from production.

Important:
  - Requires MySQL 8+ because it uses CTEs and ROW_NUMBER().
  - The final SELECT returns one row per user.
  - subject_combos is NULL when the user has not completed at least one lesson.
*/

/*
Single-statement version. Edit filter values inside the params CTE.
Use CAST(NULL AS CHAR(36)) for no filter.
*/
WITH
params AS (
    SELECT
        CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS user_id,
        CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS centre_id,
        CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS batch_id,
        CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS subject_id,
        CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS trade_id,
        CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS project_id,
        CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS program_id,
        CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS phase_id
),

active_users AS (
    SELECT
        u.id AS user_id,
        u.name AS user_name,
        u.email,
        u.mobile,
        u.type AS user_type,
        u.is_master_trainer,
        u.centre_id,
        u.project_id,
        u.organisation_id,
        u.is_ple,
        u.created_at,
        sd.batch_id,
        sd.trade_id,
        sd.educational_qualification_id,
        sd.placement_status_id,
        sd.active_year,
        sd.year_of_admission
    FROM users u
    CROSS JOIN params p
    LEFT JOIN student_details sd
        ON sd.user_id = u.id
    WHERE u.type IN (1, 2, 3, 4)
      AND u.status = 1
      AND u.deleted_at IS NULL
      AND (p.user_id IS NULL OR u.id = p.user_id)
      AND (p.centre_id IS NULL OR u.centre_id = p.centre_id)
      AND (p.batch_id IS NULL OR sd.batch_id = p.batch_id)
      AND (p.trade_id IS NULL OR sd.trade_id = p.trade_id)
),

latest_career_path AS (
    SELECT user_id, job_type_id, updated_at AS career_path_updated_at
    FROM (
        SELECT
            pcpu.user_id,
            pcpu.job_type_id,
            pcpu.updated_at,
            ROW_NUMBER() OVER (
                PARTITION BY pcpu.user_id
                ORDER BY pcpu.updated_at DESC
            ) AS rn
        FROM ple_career_path_user pcpu
        WHERE pcpu.status = 1
          AND pcpu.deleted_at IS NULL
    ) ranked
    WHERE rn = 1
),

non_ple_allocation AS (
    SELECT
        u.user_id,
        u.user_name,
        u.user_type,
        u.centre_id,
        u.project_id,
        u.batch_id,
        u.trade_id,
        NULL AS career_path_id,
        NULL AS career_path_name,
        NULL AS career_path_updated_at,
        u.is_master_trainer,
        s.id AS subject_id,
        s.name AS subject_name,
        s.is_ple AS subject_is_ple,
        s.ple_career_path_id,
        s.year_to_map,
        cs.`order` AS subject_order,
        l.id AS lesson_id,
        l.name AS lesson_name,
        l.lesson_order,
        lt.name AS lesson_type,
        CASE
            WHEN l.is_assessment = 1 OR UPPER(l.name) LIKE '%ASSESSMENT%' THEN 1
            ELSE 0
        END AS is_assessment,
        CASE
            WHEN l.student_access = 1 THEN 'student'
            WHEN l.facilitator_access = 1 THEN 'facilitator'
            WHEN l.mastertrainer_access = 1 THEN 'master'
            ELSE NULL
        END AS toolkit_type,
        t_trade.duration AS trade_duration,
        'non_ple' AS allocation_path,
        'centre_subject [-> batch_subject if batch] [-> subject_trade if trade]' AS allocation_basis
    FROM active_users u
    JOIN centre_subject cs
        ON cs.centre_id = u.centre_id
    LEFT JOIN batch_subject bs
        ON u.batch_id IS NOT NULL
       AND bs.batch_id = u.batch_id
       AND bs.subject_id = cs.subject_id
    LEFT JOIN subject_trade st
        ON u.trade_id IS NOT NULL
       AND st.trade_id = u.trade_id
       AND st.subject_id = cs.subject_id
    LEFT JOIN trades t_trade
        ON t_trade.id = u.trade_id
    JOIN subjects s
        ON s.id = cs.subject_id
       AND s.status = 1
       AND s.deleted_at IS NULL
    JOIN lessons l
        ON l.subject_id = s.id
       AND l.status = 1
       AND l.deleted_at IS NULL
       AND l.student_access = 1
       AND l.lesson_category_id = 'd78bc322-568f-4110-8e24-02ea444d48b7'
    LEFT JOIN lesson_types lt
        ON lt.id = l.lesson_type_id
    WHERE u.user_type IN (3, 4)
      AND (u.is_ple IS NULL OR u.is_ple != 1)
      AND s.is_ple IN (0, 2)
      AND (u.batch_id IS NULL OR bs.subject_id IS NOT NULL)
      AND (u.trade_id IS NULL OR st.subject_id IS NOT NULL)
      AND (
          s.year_to_map IS NULL
          OR s.year_to_map = 0
          OR t_trade.duration IS NULL
          OR s.year_to_map <= t_trade.duration
      )
      AND ((SELECT subject_id FROM params) IS NULL OR s.id = (SELECT subject_id FROM params))
),

ple_allocation AS (
    SELECT
        u.user_id,
        u.user_name,
        u.user_type,
        u.centre_id,
        u.project_id,
        u.batch_id,
        NULL AS trade_id,
        pcp.id AS career_path_id,
        pcp.name AS career_path_name,
        lcp.career_path_updated_at,
        u.is_master_trainer,
        s.id AS subject_id,
        s.name AS subject_name,
        s.is_ple AS subject_is_ple,
        s.ple_career_path_id,
        s.year_to_map,
        cs.`order` AS subject_order,
        l.id AS lesson_id,
        l.name AS lesson_name,
        l.lesson_order,
        lt.name AS lesson_type,
        CASE
            WHEN l.is_assessment = 1 OR UPPER(l.name) LIKE '%ASSESSMENT%' THEN 1
            ELSE 0
        END AS is_assessment,
        CASE
            WHEN l.student_access = 1 THEN 'student'
            WHEN l.facilitator_access = 1 THEN 'facilitator'
            WHEN l.mastertrainer_access = 1 THEN 'master'
            ELSE NULL
        END AS toolkit_type,
        t_trade.duration AS trade_duration,
        'ple' AS allocation_path,
        'centre_subject [-> subject_ple_career_path if career_path] [-> batch_subject if batch]' AS allocation_basis
    FROM active_users u
    LEFT JOIN latest_career_path lcp
        ON lcp.user_id = u.user_id
    LEFT JOIN ple_career_paths pcp
        ON pcp.id = lcp.job_type_id
       AND pcp.deleted_at IS NULL
    JOIN centre_subject cs
        ON cs.centre_id = u.centre_id
    LEFT JOIN subject_ple_career_path spcp
        ON pcp.id IS NOT NULL
       AND spcp.ple_career_path_id = pcp.id
       AND spcp.subject_id = cs.subject_id
    LEFT JOIN batch_subject bs
        ON u.batch_id IS NOT NULL
       AND bs.batch_id = u.batch_id
       AND bs.subject_id = cs.subject_id
    LEFT JOIN trades t_trade
        ON t_trade.id = u.trade_id
    JOIN subjects s
        ON s.id = cs.subject_id
       AND s.status = 1
       AND s.deleted_at IS NULL
    JOIN lessons l
        ON l.subject_id = s.id
       AND l.status = 1
       AND l.deleted_at IS NULL
       AND l.student_access = 1
       AND l.lesson_category_id = 'd78bc322-568f-4110-8e24-02ea444d48b7'
    LEFT JOIN lesson_types lt
        ON lt.id = l.lesson_type_id
    WHERE u.user_type IN (3, 4)
      AND u.is_ple = 1
      AND s.is_ple IN (1, 2)
      AND pcp.id IS NOT NULL
      AND spcp.subject_id IS NOT NULL
      AND (u.batch_id IS NULL OR bs.subject_id IS NOT NULL)
      AND (
          s.year_to_map IS NULL
          OR s.year_to_map = 0
          OR t_trade.duration IS NULL
          OR s.year_to_map <= t_trade.duration
      )
      AND ((SELECT subject_id FROM params) IS NULL OR s.id = (SELECT subject_id FROM params))
),

staff_allocation AS (
    SELECT
        u.user_id,
        u.user_name,
        u.user_type,
        u.centre_id,
        u.project_id,
        NULL AS batch_id,
        NULL AS trade_id,
        NULL AS career_path_id,
        NULL AS career_path_name,
        NULL AS career_path_updated_at,
        u.is_master_trainer,
        s.id AS subject_id,
        s.name AS subject_name,
        s.is_ple AS subject_is_ple,
        s.ple_career_path_id,
        s.year_to_map,
        cs.`order` AS subject_order,
        l.id AS lesson_id,
        l.name AS lesson_name,
        l.lesson_order,
        lt.name AS lesson_type,
        CASE
            WHEN l.is_assessment = 1 OR UPPER(l.name) LIKE '%ASSESSMENT%' THEN 1
            ELSE 0
        END AS is_assessment,
        CASE
            WHEN l.student_access = 1 THEN 'student'
            WHEN l.facilitator_access = 1 THEN 'facilitator'
            WHEN l.mastertrainer_access = 1 THEN 'master'
            ELSE NULL
        END AS toolkit_type,
        NULL AS trade_duration,
        'staff' AS allocation_path,
        'centre_subject (admin: all; facilitator: facilitator_access; master_trainer: mastertrainer_access)' AS allocation_basis
    FROM active_users u
    JOIN centre_subject cs
        ON cs.centre_id = u.centre_id
    JOIN subjects s
        ON s.id = cs.subject_id
       AND s.status = 1
       AND s.deleted_at IS NULL
    JOIN lessons l
        ON l.subject_id = s.id
       AND l.status = 1
       AND l.deleted_at IS NULL
       AND l.lesson_category_id = 'd78bc322-568f-4110-8e24-02ea444d48b7'
    LEFT JOIN lesson_types lt
        ON lt.id = l.lesson_type_id
    WHERE u.user_type IN (1, 2)
      AND s.is_ple IN (0, 1, 2)
      AND (
          u.user_type = 1
          OR (
              u.user_type = 2
              AND (u.is_master_trainer IS NULL OR u.is_master_trainer != 1)
              AND l.facilitator_access = 1
          )
          OR (
              u.user_type = 2
              AND u.is_master_trainer = 1
              AND l.mastertrainer_access = 1
          )
      )
      AND ((SELECT subject_id FROM params) IS NULL OR s.id = (SELECT subject_id FROM params))
),

allocation_union AS (
    SELECT * FROM non_ple_allocation
    UNION ALL
    SELECT * FROM ple_allocation
    UNION ALL
    SELECT * FROM staff_allocation
),

allocation_dedup AS (
    SELECT *
    FROM (
        SELECT
            au.*,
            ROW_NUMBER() OVER (
                PARTITION BY au.user_id, au.lesson_id
                ORDER BY au.career_path_updated_at DESC, au.lesson_order ASC
            ) AS rn
        FROM allocation_union au
    ) ranked
    WHERE rn = 1
),

allocation_filtered AS (
    SELECT *
    FROM allocation_dedup
    WHERE COALESCE(LOWER(TRIM(lesson_type)), '') NOT IN ('pdf', 'mp4', 'pdf web')
),

learner_completion AS (
    SELECT
        la.user_id,
        la.lesson_id,
        MAX(la.score) AS score,
        MAX(la.rating) AS rating,
        NULL AS data_from,
        SUM(la.duration) AS duration
    FROM learning_activities la
    JOIN active_users u
        ON u.user_id = la.user_id
       AND u.user_type IN (3, 4)
    WHERE la.completed = 1
    GROUP BY la.user_id, la.lesson_id
),

staff_completion AS (
    SELECT
        fla.user_id,
        fla.lesson_id,
        MAX(fla.score) AS score,
        MAX(fla.rating) AS rating,
        NULL AS data_from,
        SUM(fla.duration) AS duration
    FROM facilitator_learning_activities fla
    JOIN active_users u
        ON u.user_id = fla.user_id
       AND u.user_type NOT IN (3, 4)
    WHERE fla.completed = 1
    GROUP BY fla.user_id, fla.lesson_id
),

completion_dedup AS (
    SELECT user_id, lesson_id, score, rating, data_from, duration
    FROM (
        SELECT
            c.*,
            ROW_NUMBER() OVER (
                PARTITION BY c.user_id, c.lesson_id
                ORDER BY c.score DESC
            ) AS rn
        FROM (
            SELECT * FROM learner_completion
            UNION ALL
            SELECT * FROM staff_completion
        ) c
    ) ranked
    WHERE rn = 1
),

merged AS (
    SELECT
        a.user_id,
        a.user_name,
        a.user_type,
        a.centre_id,
        a.project_id,
        a.batch_id,
        a.trade_id,
        a.career_path_id,
        a.career_path_name,
        a.is_master_trainer,
        a.subject_id,
        a.subject_name,
        a.subject_is_ple,
        a.ple_career_path_id,
        a.year_to_map,
        a.subject_order,
        a.lesson_id,
        a.lesson_name,
        a.lesson_order,
        a.lesson_type,
        a.is_assessment,
        a.toolkit_type,
        a.trade_duration,
        a.allocation_path,
        a.allocation_basis,
        c.score,
        c.rating,
        c.data_from,
        c.duration,
        CASE WHEN c.user_id IS NULL THEN 0 ELSE 1 END AS completed
    FROM allocation_filtered a
    LEFT JOIN completion_dedup c
        ON c.user_id = a.user_id
       AND c.lesson_id = a.lesson_id
),

user_summary AS (
    SELECT
        user_id,
        COUNT(lesson_id) AS total_allocated,
        SUM(CASE WHEN is_assessment = 0 THEN 1 ELSE 0 END) AS total_lessons_allocated,
        SUM(CASE WHEN is_assessment = 1 THEN 1 ELSE 0 END) AS total_assessments_allocated,
        SUM(completed) AS total_completed,
        SUM(CASE WHEN completed = 1 AND is_assessment = 0 THEN 1 ELSE 0 END) AS total_lessons_completed,
        SUM(CASE WHEN completed = 1 AND is_assessment = 1 THEN 1 ELSE 0 END) AS total_assessments_completed,
        ROUND(SUM(completed) / NULLIF(COUNT(lesson_id), 0) * 100, 2) AS completion_pct
    FROM merged
    GROUP BY user_id
),

subject_allocation_summary AS (
    SELECT
        user_id,
        subject_id,
        COUNT(lesson_id) AS subj_total_allocated,
        SUM(CASE WHEN is_assessment = 0 THEN 1 ELSE 0 END) AS subj_lessons_allocated,
        SUM(CASE WHEN is_assessment = 1 THEN 1 ELSE 0 END) AS subj_assessments_allocated
    FROM merged
    GROUP BY user_id, subject_id
),

subject_completion_summary AS (
    SELECT
        user_id,
        subject_id,
        COUNT(lesson_id) AS subj_total_completed,
        SUM(CASE WHEN is_assessment = 0 THEN 1 ELSE 0 END) AS subj_lessons_completed,
        SUM(CASE WHEN is_assessment = 1 THEN 1 ELSE 0 END) AS subj_assessments_completed
    FROM merged
    WHERE completed = 1
    GROUP BY user_id, subject_id
),

completed_lesson_rows AS (
    SELECT
        m.*,
        us.total_allocated,
        us.total_lessons_allocated,
        us.total_assessments_allocated,
        us.total_completed,
        us.total_lessons_completed,
        us.total_assessments_completed,
        us.completion_pct,
        sas.subj_total_allocated,
        sas.subj_lessons_allocated,
        sas.subj_assessments_allocated,
        COALESCE(scs.subj_total_completed, 0) AS subj_total_completed,
        COALESCE(scs.subj_lessons_completed, 0) AS subj_lessons_completed,
        COALESCE(scs.subj_assessments_completed, 0) AS subj_assessments_completed
    FROM merged m
    JOIN user_summary us
        ON us.user_id = m.user_id
    JOIN subject_allocation_summary sas
        ON sas.user_id = m.user_id
       AND sas.subject_id = m.subject_id
    LEFT JOIN subject_completion_summary scs
        ON scs.user_id = m.user_id
       AND scs.subject_id = m.subject_id
    WHERE m.completed = 1
),

zero_completion_subject_rows AS (
    SELECT
        z.user_id,
        z.user_name,
        z.user_type,
        z.centre_id,
        z.project_id,
        z.batch_id,
        z.trade_id,
        z.career_path_id,
        z.career_path_name,
        z.is_master_trainer,
        z.subject_id,
        z.subject_name,
        z.subject_is_ple,
        z.ple_career_path_id,
        z.year_to_map,
        z.subject_order,
        NULL AS lesson_id,
        NULL AS lesson_name,
        NULL AS lesson_order,
        NULL AS lesson_type,
        NULL AS is_assessment,
        NULL AS toolkit_type,
        z.trade_duration,
        z.allocation_path,
        z.allocation_basis,
        NULL AS score,
        NULL AS rating,
        NULL AS data_from,
        NULL AS duration,
        0 AS completed,
        us.total_allocated,
        us.total_lessons_allocated,
        us.total_assessments_allocated,
        us.total_completed,
        us.total_lessons_completed,
        us.total_assessments_completed,
        us.completion_pct,
        sas.subj_total_allocated,
        sas.subj_lessons_allocated,
        sas.subj_assessments_allocated,
        0 AS subj_total_completed,
        0 AS subj_lessons_completed,
        0 AS subj_assessments_completed
    FROM (
        SELECT
            m.*,
            ROW_NUMBER() OVER (
                PARTITION BY m.user_id, m.subject_id
                ORDER BY m.subject_order ASC
            ) AS subject_rn
        FROM merged m
        JOIN user_summary us_check
            ON us_check.user_id = m.user_id
           AND us_check.total_completed = 0
    ) z
    JOIN user_summary us
        ON us.user_id = z.user_id
    JOIN subject_allocation_summary sas
        ON sas.user_id = z.user_id
       AND sas.subject_id = z.subject_id
    WHERE z.subject_rn = 1
),

lesson_output AS (
    SELECT * FROM completed_lesson_rows
    UNION ALL
    SELECT * FROM zero_completion_subject_rows
),

no_allocation_user_rows AS (
    SELECT
        u.user_id,
        u.user_name,
        u.user_type,
        u.is_master_trainer,
        u.centre_id,
        u.project_id,
        u.is_ple,
        u.batch_id,
        u.trade_id,
        0 AS total_allocated,
        0 AS total_lessons_allocated,
        0 AS total_assessments_allocated,
        0 AS total_completed,
        0 AS total_lessons_completed,
        0 AS total_assessments_completed,
        0.00 AS completion_pct,
        0 AS completed
    FROM active_users u
    LEFT JOIN (
        SELECT DISTINCT user_id FROM allocation_filtered
    ) allocated
        ON allocated.user_id = u.user_id
    WHERE allocated.user_id IS NULL
),

subject_output AS (
    SELECT
        lo.user_id,
        lo.user_name,
        lo.user_type,
        lo.centre_id,
        lo.project_id,
        lo.batch_id,
        lo.trade_id,
        lo.career_path_id,
        lo.career_path_name,
        lo.subject_id,
        lo.subject_name,
        lo.subject_is_ple,
        lo.year_to_map,
        lo.allocation_basis,
        lo.total_allocated,
        lo.total_lessons_allocated,
        lo.total_assessments_allocated,
        lo.total_completed,
        lo.total_lessons_completed,
        lo.total_assessments_completed,
        lo.completion_pct,
        lo.subj_total_allocated,
        lo.subj_lessons_allocated,
        lo.subj_assessments_allocated,
        lo.subj_total_completed,
        lo.subj_lessons_completed,
        lo.subj_assessments_completed,
        ROUND(AVG(lo.score), 2) AS avg_score,
        ROUND(AVG(lo.rating), 2) AS avg_rating,
        AVG(lo.duration) AS avg_duration,
        SUM(lo.duration) AS total_duration
    FROM lesson_output lo
    WHERE lo.subject_id IS NOT NULL
    GROUP BY
        lo.user_id,
        lo.subject_id,
        lo.user_name,
        lo.user_type,
        lo.centre_id,
        lo.project_id,
        lo.batch_id,
        lo.trade_id,
        lo.career_path_id,
        lo.career_path_name,
        lo.subject_name,
        lo.subject_is_ple,
        lo.year_to_map,
        lo.allocation_basis,
        lo.total_allocated,
        lo.total_lessons_allocated,
        lo.total_assessments_allocated,
        lo.total_completed,
        lo.total_lessons_completed,
        lo.total_assessments_completed,
        lo.completion_pct,
        lo.subj_total_allocated,
        lo.subj_lessons_allocated,
        lo.subj_assessments_allocated,
        lo.subj_total_completed,
        lo.subj_lessons_completed,
        lo.subj_assessments_completed
),

active_centres AS (
    SELECT
        c.id AS centre_id,
        c.organisation_id,
        c.name AS centre_name,
        c.centre_type_id,
        c.ple_enabled
    FROM centres c
    CROSS JOIN params p
    WHERE c.status = 1
      AND c.deleted_at IS NULL
      AND (p.centre_id IS NULL OR c.id = p.centre_id)
),

centre_project_map AS (
    SELECT
        cp.centre_id,
        cp.project_id
    FROM centre_project cp
    CROSS JOIN params p
    WHERE (p.project_id IS NULL OR cp.project_id = p.project_id)
),

active_projects AS (
    SELECT
        prj.id AS project_id,
        prj.program_id,
        prj.name AS project_name
    FROM projects prj
    CROSS JOIN params p
    WHERE prj.status = 1
      AND prj.deleted_at IS NULL
      AND (p.program_id IS NULL OR prj.program_id = p.program_id)
),

active_programs AS (
    SELECT
        prg.id AS program_id,
        prg.name AS program_name
    FROM programs prg
    WHERE prg.status = 1
      AND prg.deleted_at IS NULL
),

main_centre_project AS (
    SELECT DISTINCT
        prg.program_name,
        prj.project_id,
        prj.project_name,
        c.centre_id,
        c.ple_enabled
    FROM active_centres c
    JOIN centre_project_map cp
        ON cp.centre_id = c.centre_id
    JOIN active_projects prj
        ON prj.project_id = cp.project_id
    LEFT JOIN active_programs prg
        ON prg.program_id = prj.program_id
),

centre_batch AS (
    SELECT
        u.user_id AS cb_user_id,
        u.centre_id AS cb_centre_id,
        u.batch_id AS cb_batch_id
    FROM active_users u
    WHERE u.batch_id IS NOT NULL
    GROUP BY u.user_id, u.batch_id, u.centre_id
),

batch_phase_source AS (
    SELECT
        ph.id AS p_phase_id,
        cb.cb_batch_id AS p_batch_id,
        ph.name AS phase_name,
        cb.cb_centre_id AS p_centre_id,
        pp.project_id AS p_project_id,
        cb.cb_user_id AS p_user_id
    FROM centre_batch cb
    JOIN batches b
        ON b.id = cb.cb_batch_id
    JOIN batch_phase bp
        ON bp.batch_id = b.id
    JOIN centre_phase cp
        ON cp.phase_id = bp.phase_id
       AND cp.centre_id = cb.cb_centre_id
    JOIN phases ph
        ON ph.id = cp.phase_id
    JOIN phase_project pp
        ON pp.phase_id = ph.id
    JOIN centres c
        ON c.id = cp.centre_id
    CROSS JOIN params p
    WHERE b.deleted_at IS NULL
      AND cp.deleted_at IS NULL
      AND bp.deleted_at IS NULL
      AND ph.deleted_at IS NULL
      AND (p.project_id IS NULL OR pp.project_id = p.project_id)
      AND (p.phase_id IS NULL OR ph.id = p.phase_id)
),

direct_phase_user_source AS (
    SELECT
        ph.id AS p_phase_id,
        CAST(NULL AS CHAR(36)) COLLATE utf8mb4_unicode_ci AS p_batch_id,
        ph.name AS phase_name,
        c.id AS p_centre_id,
        pp.project_id AS p_project_id,
        u.user_id AS p_user_id
    FROM phase_users pu
    JOIN active_users u
        ON u.user_id = pu.user_id
    JOIN centre_phase cp
        ON cp.centre_id = u.centre_id
    JOIN phases ph
        ON ph.id = cp.phase_id
    JOIN phase_project pp
        ON pp.phase_id = ph.id
    JOIN centres c
        ON c.id = u.centre_id
    CROSS JOIN params p
    WHERE pu.deleted_at IS NULL
      AND cp.deleted_at IS NULL
      AND ph.deleted_at IS NULL
      AND (p.project_id IS NULL OR pp.project_id = p.project_id)
      AND (p.phase_id IS NULL OR ph.id = p.phase_id)
    GROUP BY ph.id, c.id, pp.project_id, u.user_id
),

main_phases AS (
    SELECT * FROM batch_phase_source
    UNION ALL
    SELECT * FROM direct_phase_user_source
),

user_project_phase_rows AS (
    SELECT DISTINCT
        u.user_id,
        cp.program_name,
        cp.project_id,
        cp.project_name,
        ph.p_phase_id,
        ph.phase_name
    FROM active_users u
    LEFT JOIN main_centre_project cp
        ON cp.centre_id = u.centre_id
    LEFT JOIN main_phases ph
        ON ph.p_batch_id   = u.batch_id
       AND ph.p_centre_id  = u.centre_id
       AND ph.p_project_id = cp.project_id
       AND ph.p_user_id    = u.user_id
    WHERE cp.project_id IS NOT NULL
),

user_project_combos AS (
    SELECT
        up.user_id,
        CASE
            WHEN MAX(up.project_id) IS NULL THEN NULL
            ELSE JSON_ARRAYAGG(
                JSON_OBJECT(
                    'prog_name', up.program_name,
                    'project_id', up.project_id,
                    'proj_name', up.project_name,
                    'p_phase_id', up.p_phase_id,
                    'phase', up.phase_name
                )
            )
        END AS project_combos
    FROM user_project_phase_rows up
    GROUP BY up.user_id
),

user_subject_combos AS (
    SELECT
        so.user_id,
        CASE
            WHEN MAX(so.total_lessons_completed) > 0 THEN
                JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'subject_id', so.subject_id,
                        'subject_name', so.subject_name,
                        'avg_score', so.avg_score,
                        'avg_rating', so.avg_rating,
                        'completed_lessons_and_assessments', so.subj_total_completed,
                        'allocated_lessons_and_assessments', so.subj_total_allocated,
                        'allocated_assessments', so.subj_assessments_allocated,
                        'allocated_lessons', so.subj_lessons_allocated,
                        'completed_assessments', so.subj_assessments_completed,
                        'completed_lessons', so.subj_lessons_completed,
                        'year_category', so.year_to_map
                    )
                )
            ELSE NULL
        END AS subject_combos,
        MAX(so.total_allocated) AS total_allocated,
        MAX(so.total_lessons_allocated) AS total_lessons_allocated,
        MAX(so.total_assessments_allocated) AS total_assessments_allocated,
        MAX(so.total_completed) AS total_completed,
        MAX(so.total_lessons_completed) AS total_lessons_completed,
        MAX(so.total_assessments_completed) AS total_assessments_completed,
        MAX(so.completion_pct) AS completion_pct
    FROM subject_output so
    GROUP BY so.user_id
),

one_user_row AS (
    SELECT
        au.user_id,
        MAX(au.user_name) AS user_name,
        MAX(au.email) AS email,
        MAX(au.mobile) AS mobile,
        MAX(au.user_type) AS user_type,
        MAX(au.is_master_trainer) AS is_master_trainer,
        MAX(au.centre_id) AS centre_id,
        MAX(au.project_id) AS project_id,
        MAX(au.organisation_id) AS organisation_id,
        MAX(au.is_ple) AS is_ple,
        MAX(au.created_at) AS created_at,
        MAX(au.batch_id) AS batch_id,
        MAX(au.trade_id) AS trade_id,
        MAX(au.educational_qualification_id) AS educational_qualification_id,
        MAX(au.placement_status_id) AS placement_status_id,
        MAX(au.active_year) AS active_year,
        MAX(au.year_of_admission) AS year_of_admission
    FROM active_users au
    GROUP BY au.user_id
)

SELECT
    u.user_id,
    -- u.user_name,
    -- u.email,
    -- u.mobile,
    u.user_type,
    -- u.is_master_trainer,
    u.centre_id,
    -- u.project_id,
    u.organisation_id,
    u.is_ple,
    u.created_at,
    u.batch_id,
    u.trade_id,
    -- u.educational_qualification_id,
    -- u.placement_status_id,
    -- u.active_year,
    -- u.year_of_admission,
    COALESCE(usc.total_allocated, 0) AS total_allocated,
    COALESCE(usc.total_lessons_allocated, 0) AS total_lessons_allocated,
    COALESCE(usc.total_assessments_allocated, 0) AS total_assessments_allocated,
    COALESCE(usc.total_completed, 0) AS total_completed,
    COALESCE(usc.total_lessons_completed, 0) AS total_lessons_completed,
    COALESCE(usc.total_assessments_completed, 0) AS total_assessments_completed,
    COALESCE(usc.completion_pct, 0.00) AS completion_pct,
    upc.project_combos,
    usc.subject_combos
FROM one_user_row u
LEFT JOIN user_project_combos upc
    ON upc.user_id = u.user_id
LEFT JOIN user_subject_combos usc
    ON usc.user_id = u.user_id
ORDER BY u.user_id;
