#' Acquire a Redis Lock with Timeout
#'
#' This function attempts to acquire a lock in Redis with a specified
#' timeout for acquiring the lock and a timeout for how long the lock
#' should be held.
#'
#' @param conn A connection object to Redis created with the `redux` package.
#' @param lockname A string representing the name of the lock to acquire.
#' @param acquire_timeout Numeric value (in seconds) specifying how long
#'   to wait to acquire the lock. Default is 10 seconds.
#' @param lock_timeout Numeric value (in seconds) specifying how long
#'   the lock should remain valid. Default is 10 seconds.
#' @return A string containing the unique identifier for the lock if
#'   acquired successfully; otherwise, returns NULL.
#' @examples
#' \dontrun{
#' conn <- redux::redis()  # Create a connection to Redis
#' lock_id <- acquire_lock_with_timeout(conn, "my_lock_name")
#' if (!is.null(lock_id)) {
#'   # Lock acquired successfully, proceed with critical section
#'   # Release the lock or perform actions here
#'   }
#' }
#' @export
acquire_lock_with_timeout <- function(conn, lockname, acquire_timeout = 10, lock_timeout = 10) {
  # Generate a unique identifier for the lock
  identifier <- as.character(uuid::UUIDgenerate())

  # Prefix the lockname to indicate it's a lock
  lockname <- paste0('lock:', lockname)

  # Round up the lock timeout to the nearest whole number
  lock_timeout <- ceiling(lock_timeout)

  # Initialize a flag to track if the lock has been acquired
  acquired <- FALSE

  # Calculate the end time for acquiring the lock
  end <- Sys.time() + acquire_timeout

  # Attempt to acquire the lock until the timeout is reached or the lock is acquired
  while (Sys.time() < end && !acquired) {
    # Call the Redis command to attempt to acquire the lock
    acquired <- !is.null(conn$EVAL("
      if redis.call('exists', KEYS[1]) == 0 then
        return redis.call('setex', KEYS[1], ARGV[1], ARGV[2])
      end
      return nil
    ", 1L, lockname, c(lock_timeout, identifier)))

    # If not acquired, sleep for a short duration to avoid busy-waiting
    Sys.sleep(0.1 * (!acquired))
  }

  # Return whether the lock was acquired and the identifier if acquired
  if(acquired) return(identifier)
  NULL
}


#' Release a Redis Lock
#'
#' This function attempts to release a lock in Redis using the specified
#' identifier. It checks if the lock belongs to the identifier before
#' releasing it.
#'
#' @param conn A connection object to Redis created with the `redux` package.
#' @param lockname A string representing the name of the lock to release.
#' @param identifier A string representing the unique identifier of the lock.
#' @return A logical value indicating whether the lock was released successfully.
#' @examples
#' \dontrun{
#' conn <- redux::redis()  # Create a connection to Redis
#' success <- release_lock(conn, "my_lock_name", lock_id)
#' }
#' @export
release_lock <- function(conn, lockname, identifier) {
  # Prefix the lockname to indicate it's a lock
  lockname <- paste0('lock:', lockname)

  # Call the Redis command to attempt to release the lock
  result <- !is.null(conn$EVAL("
    if redis.call('get', KEYS[1]) == ARGV[1] then
      return redis.call('del', KEYS[1]) or true
    end
    return false
  ", 1L, lockname, identifier))

  # Return whether the lock was released successfully
  return(result)
}

test_redlock <- function(host, uselock, n_tests = 100, n_agents = 4, lockname = "testing") {
  conn <- redux::hiredis(host = host)
  conn$SET("count", 0L)
  list(
    parallel::mclapply(
      1:n_agents,
      function(n) {
        conn <- redux::hiredis(host = host)
        sapply(
          1:n_tests,
          function(x) {
            if(uselock) {
              result <- acquire_lock_with_timeout(conn, lockname)
              if(length(result == 1)) {
                conn$SET("count", as.integer(conn$GET("count"))+1)
                release_lock(conn, lockname, result)
              }
            } else {
              conn$SET("count", as.integer(conn$GET("count"))+1)
              result <- TRUE
            }
            result
          }
        )
      }
    ),
    conn$GET("count"))
}
