param(
  [string]$OutCsv = "C:\Users\nicol\NH_Reps\nh_house_ids.csv"
)

# --- config ---
$Key = $env:OPENSTATES_API_KEY
if (-not $Key) { throw "Set OPENSTATES_API_KEY first." }

$Headers = @{ "X-API-KEY" = $Key; "Accept" = "application/json" }
$Base    = "https://v3.openstates.org/people"
$PerPage = 50                                    # API cap
$Cache   = Join-Path $PSScriptRoot ".cache_ids"  # page cache so we can resume
New-Item -ItemType Directory -Force -Path $Cache | Out-Null

$all  = @()
$page = 1
while ($true) {

  $cacheFile = Join-Path $Cache ("page_{0}.json" -f $page)

  if (Test-Path $cacheFile) {
    $json = Get-Content $cacheFile -Raw | ConvertFrom-Json
  } else {
    $uri = "$Base?jurisdiction=New%20Hampshire&org_classification=lower&per_page=$PerPage&page=$page"

    while ($true) {
      try {
        $resp = Invoke-WebRequest -Headers $Headers -Uri $uri -ErrorAction Stop
        $json = $resp.Content | ConvertFrom-Json
        $resp.Content | Out-File -FilePath $cacheFile -Encoding utf8
        break
      } catch [System.Net.WebException] {
        $res  = $_.Exception.Response
        $code = $null
        try { $code = [int]$res.StatusCode.value__ } catch {}
        if ($code -eq 429) {
          $ra = [int]($res.Headers['Retry-After'] | Select-Object -First 1)
          if (-not $ra) { $ra = 20 }
          Write-Host ("[page {0}] 429 — sleeping {1}s…" -f $page,$ra) -ForegroundColor Yellow
          Start-Sleep -Seconds $ra
          continue
        } else {
          throw
        }
      }
    }
  }

  $results = $json.results
  $count   = ($results | Measure-Object).Count
  Write-Host ("page {0}: {1} results (total so far {2})" -f $page,$count,($all.Count + $count))

  foreach ($rec in $results) {
    $p = $rec.person
    $party = if ($p.party -is [System.Array]) { $p.party[0].name } else { $p.party }
    $district = if ($rec.district) { $rec.district }
               elseif ($p.current_role.district) { $p.current_role.district }
               else { "" }

    # best-effort email/phone
    $email = ""
    if ($p.email_addresses) { $email = ($p.email_addresses | ? address | Select -First 1 -Expand address) }
    if (-not $email -and $p.emails) { $email = ($p.emails | Select -First 1) }

    $phone = ""
    if ($p.offices) { $phone = ($p.offices | ? voice | Select -First 1 -Expand voice) }

    $all += [pscustomobject]@{
      openstates_person_id = $p.id
      name                 = $p.name
      district             = $district
      party                = $party
      email                = $email
      phone                = $phone
    }
  }

  if ($count -lt $PerPage) { break }   # last page
  $page++
  Start-Sleep -Seconds 2               # gentle throttle between pages
}

# de-dupe by (district,name) and write
$all = $all | Sort-Object district,name -Unique
$all | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
Write-Host ("Wrote {0} ({1} rows)" -f $OutCsv, $all.Count)
