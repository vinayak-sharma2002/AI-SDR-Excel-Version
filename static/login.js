function login() {
  let user = document.getElementById("username").value.trim();
  let pass = document.getElementById("password").value.trim();
  let error = document.getElementById("error-msg");

  let emailPattern = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}$/;

  if (!emailPattern.test(user)) {
    error.textContent = "Please enter a valid email address.";
    return;
  }

  if (pass.length < 6) {
    error.textContent = "Password must be at least 6 characters.";
    return;
  }

  const authHeader = 'Basic ' + btoa(user + ':' + pass);
  
  fetch('/login', {
    method: 'POST',
    headers: {
      'Authorization': authHeader,
      'Content-Type': 'application/json'
    },
    credentials: 'include'
  })
  .then(response => {
    if (response.ok) {
      window.location.href = "/upload";
    } else if (response.status === 401) {
      error.textContent = "Invalid email or password.";
    } else {
      error.textContent = "Login failed. Please try again.";
    }
  })
  .catch(err => {
    console.error('Login error:', err);
    error.textContent = "Login failed. Please try again.";
  });
}
