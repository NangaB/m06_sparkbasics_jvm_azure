import ch.hsr.geohash.GeoHash
import com.byteowls.jopencage.JOpenCageGeocoder
import com.byteowls.jopencage.model.JOpenCageForwardRequest

import scala.Double.NaN

object GeoService {
  private val TWENTY_FIVE_KM_PRECISION = 2
  private val GEOHASH_LENGTH = 4
  private val APIKey = "f8d63dc620ca4b2da1197eb2c29ae780"
  private val jOpenCageGeocoder = new JOpenCageGeocoder(APIKey)

  def getGeohash(latitude: Double, longitude: Double): String =
    if (latitude.isNaN || longitude.isNaN)
      null
    else
      GeoHash.withCharacterPrecision(latitude, longitude, GEOHASH_LENGTH).toBase32

  def getGeolocation(country: String, city: String, address: String): (Double, Double) = {
    val prettifiedAddress = address.replaceAll("(\\(.*\\))", "")
    val location = jOpenCageGeocoder
      .forward(buildRequest(country, city, prettifiedAddress))
      .getFirstPosition

    if (location == null) (NaN, NaN) else (location.getLat, location.getLng)
  }

  private def buildRequest(country: String, city: String, prettifiedAddress: String) = {
    val request = new JOpenCageForwardRequest(s"$country $city $prettifiedAddress")
    request.setRestrictToCountryCode(country)
    request.setNoAnnotations(true)
    request.setNoDedupe(true)
    request.setMinConfidence(TWENTY_FIVE_KM_PRECISION)
    request
  }

}
